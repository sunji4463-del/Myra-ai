package com.myra.assistant.ai

import android.content.Context
import android.content.SharedPreferences
import android.util.Base64
import android.util.Log
import com.myra.assistant.BuildConfig
import kotlinx.coroutines.*
import okhttp3.*
import okhttp3.WebSocket
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

class GeminiLiveClient(
  private val context: Context,
  private val prefs: SharedPreferences
) {

  companion object {
    const val TAG = "GeminiLiveClient"
    const val SESSION_RENEW_AFTER_MS = 540_000L
    const val KEEPALIVE_INTERVAL_MS = 8_000L
    const val RECONNECT_DELAY_MS = 3_000L
  }

  private val client = OkHttpClient.Builder()
    .pingInterval(30, TimeUnit.SECONDS)
    .connectTimeout(30, TimeUnit.SECONDS)
    .readTimeout(0, TimeUnit.SECONDS)
    .writeTimeout(30, TimeUnit.SECONDS)
    .retryOnConnectionFailure(true)
    .build()

  private var webSocket: WebSocket? = null
  private var sessionJob: Job? = null
  private var keepaliveJob: Job? = null
  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

  var onSetupComplete: (() -> Unit)? = null
  var onInputTranscript: ((String) -> Unit)? = null
  var onOutputTranscript: ((String) -> Unit)? = null
  var onAudioData: ((ByteArray) -> Unit)? = null
  var onTurnComplete: (() -> Unit)? = null
  var onError: ((String) -> Unit)? = null
  var onToolCall: ((String, String, JSONObject) -> Unit)? = null

  var isConnected = false
    private set
  var isSetupComplete = false
    private set
  private var isSpeaking = false
  private val silentPcm: ByteArray by lazy { ByteArray(3200) { 0 } }
  // Buffer audio chunks jab tak setup complete nahi hota
  private val audioBuffer = java.util.ArrayList<ByteArray>()
  private var bufferAudio = false
  @Volatile private var connecting = false

  fun connect() {
    if (isConnected) {
      Log.d(TAG, "Already connected")
      return
    }
    isSetupComplete = false
    bufferAudio = true
    audioBuffer.clear()
    // Priority: manual key (SharedPreferences) > BuildConfig > error
    val manualKey = prefs.getString("api_key", null)?.trim()?.takeIf { it.isNotBlank() }
    val buildKey = BuildConfig.GEMINI_API_KEY.trim().takeIf { it.isNotBlank() }
    val apiKey = manualKey ?: buildKey
    if (apiKey == null) {
      Log.w(TAG, "No API key found. Set in Settings or local.properties")
      onError?.invoke("API Key missing — go to Settings and add your Gemini key")
      return
    }
    Log.d(TAG, "Connecting with ${if (manualKey != null) "manual key" else "BuildConfig key"}")

    if (connecting) return
    connecting = true
    scope.launch {
      try {
        val savedModel = prefs.getString("gemini_model", null)
        val model = resolveModel(apiKey, savedModel)
        Log.d(TAG, "Using model: $model")
        openSocket(apiKey, model)
      } catch (e: Exception) {
        Log.e(TAG, "connect error", e)
        onError?.invoke("Connect error: ${e.message}")
      } finally {
        connecting = false
      }
    }
  }

  // Google ke server se available Live (bidiGenerateContent) model khud dhundho
  private fun resolveModel(apiKey: String, saved: String?): String {
    val fallback = saved ?: "models/gemini-2.5-flash-native-audio-preview-12-2025"
    return try {
      val req = Request.Builder()
        .url("https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000&key=$apiKey")
        .build()
      val body = client.newCall(req).execute().use { it.body?.string() } ?: return fallback
      val arr = JSONObject(body).optJSONArray("models") ?: return fallback
      val live = ArrayList<String>()
      for (i in 0 until arr.length()) {
        val m = arr.getJSONObject(i)
        val methods = m.optJSONArray("supportedGenerationMethods") ?: continue
        for (j in 0 until methods.length()) {
          if (methods.getString(j) == "bidiGenerateContent") {
            val n = m.getString("name")
            // Transcribe/TTS/translate model voice-chat ke liye nahi hain, aur jo model pehle fail hue unhe skip karo
            val skip = n.contains("transcribe") || n.contains("tts") || n.contains("translate") ||
              badModels().contains(n)
            if (!skip) live.add(n)
            break
          }
        }
      }
      Log.d(TAG, "Live models: $live")
      // Sabse tez model pehle: half-cascade "live" model, phir native-audio
      when {
        live.isEmpty() -> fallback
        else -> live.firstOrNull { it.contains("live") && !it.contains("native-audio") && !it.contains("preview-09") }
          ?: live.firstOrNull { it.contains("native-audio") && !it.contains("preview-09") }
          ?: live.firstOrNull { it.contains("native-audio") }
          ?: live.first()
      }
    } catch (e: Exception) {
      Log.e(TAG, "resolveModel failed", e)
      fallback
    }
  }

  @Volatile private var currentModel: String = ""

  private fun badModels(): Set<String> =
    HashSet(prefs.getStringSet("bad_models", emptySet()) ?: emptySet())

  private fun markModelBad(model: String) {
    if (model.isBlank()) return
    val set = HashSet(badModels())
    set.add(model)
    prefs.edit().putStringSet("bad_models", set).apply()
  }

  private fun openSocket(apiKey: String, model: String) {
    currentModel = model
    val voice = prefs.getString("gemini_voice", "Aoede") ?: "Aoede"
    val userName = prefs.getString("user_name", "Sir") ?: "Sir"
    val personality = prefs.getString("personality_mode", "gf") ?: "gf"

    val systemPrompt = buildSystemPrompt(userName, personality)

    val wsHost = "generativelanguage.googleapis.com"
    val wsPath = "/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent"
    val wsUrl = "wss" + "://" + wsHost + wsPath + "?key=" + apiKey

    val request = Request.Builder()
      .url(wsUrl)
      .build()

    webSocket?.cancel()
    sessionJob?.cancel()
    keepaliveJob?.cancel()
    webSocket = client.newWebSocket(request, object : WebSocketListener() {
      override fun onOpen(webSocket: WebSocket, response: Response) {
        Log.d(TAG, "WebSocket opened")
        isConnected = true
        sendSetup(model, voice, systemPrompt)
      }

      override fun onMessage(webSocket: WebSocket, text: String) {
        handleMessage(text)
      }

      // Gemini Live aksar JSON ko binary frame mein bhejta hai
      override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
        handleMessage(bytes.utf8())
      }

      override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
        Log.d(TAG, "WebSocket closing: $code / $reason")
        if (code != 1000) onError?.invoke("Server closed: $code $reason")
        // Model support nahi karta (1007/1008)? Usse yaad rakho, agli baar dusra model chuno
        if (code == 1007 || code == 1008) markModelBad(currentModel)
        isConnected = false
        isSetupComplete = false
      }

      override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        if (webSocket !== this@GeminiLiveClient.webSocket) return
        Log.d(TAG, "WebSocket closed: $code / $reason")
        isConnected = false
        isSetupComplete = false
        scheduleReconnect()
      }

      override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        if (webSocket !== this@GeminiLiveClient.webSocket) return
        Log.e(TAG, "WebSocket failure: ${t.message}")
        isConnected = false
        isSetupComplete = false
        onError?.invoke("Connection failed: ${t.message}")
        scheduleReconnect()
      }
    })

    startSessionRenewal()
    startKeepalive()
  }

  private fun sendSetup(model: String, voice: String, systemPrompt: String) {
    val setup = JSONObject().apply {
      put("setup", JSONObject().apply {
        put("model", model)
        put("system_instruction", JSONObject().apply {
          put("parts", JSONArray().apply {
            put(JSONObject().put("text", systemPrompt))
          })
        })
        put("generation_config", JSONObject().apply {
          put("response_modalities", JSONArray().apply { put("AUDIO") })
          put("speech_config", JSONObject().apply {
            put("voice_config", JSONObject().apply {
              put("prebuilt_voice_config", JSONObject().apply {
                put("voice_name", voice)
              })
            })
          })
          put("temperature", 0.7)
        })
        put("output_audio_transcription", JSONObject())
        put("input_audio_transcription", JSONObject())
        put("tools", buildTools())
        // Fast reply: user ke chup hote hi jaldi jawab shuru ho
        put("realtime_input_config", JSONObject().apply {
          put("automatic_activity_detection", JSONObject().apply {
            put("disabled", false)
            put("start_of_speech_sensitivity", "START_SENSITIVITY_HIGH")
            put("end_of_speech_sensitivity", "END_SENSITIVITY_HIGH")
            put("prefix_padding_ms", 20)
            put("silence_duration_ms", 250)
          })
        })
      })
    }
    webSocket?.send(setup.toString())
  }

  private fun fn(name: String, desc: String, props: Map<String, String>, required: List<String>): JSONObject {
    return JSONObject().apply {
      put("name", name)
      put("description", desc)
      put("parameters", JSONObject().apply {
        put("type", "OBJECT")
        put("properties", JSONObject().apply {
          for ((k, v) in props) {
            put(k, JSONObject().apply {
              put("type", "STRING")
              put("description", v)
            })
          }
        })
        put("required", JSONArray(required))
      })
    }
  }

  // Gemini ko phone control ke tools batao
  private fun buildTools(): JSONArray {
    val decls = JSONArray().apply {
      put(fn("open_app", "Open an app installed on the user's phone (e.g. WhatsApp, YouTube, Chrome, Facebook, Settings).",
        mapOf("app_name" to "Name of the app, e.g. whatsapp"), listOf("app_name")))
      put(fn("close_app", "Close the current app and go to the home screen.", emptyMap(), emptyList()))
      put(fn("call_contact", "Make a phone call to a saved contact name or a phone number.",
        mapOf("name" to "Contact name as saved in the phone", "number" to "Phone number, if the user said digits"), emptyList()))
      put(fn("send_sms", "Send an SMS text message to a contact or phone number.",
        mapOf("name" to "Contact name", "number" to "Phone number, optional", "message" to "The message text to send"), listOf("message")))
      put(fn("send_whatsapp", "Write and send a WhatsApp message to a contact or phone number.",
        mapOf("name" to "Contact name", "number" to "Phone number, optional", "message" to "The message text to send"), listOf("message")))
      put(fn("web_search", "Search the web on Google in the phone's browser.",
        mapOf("query" to "What to search for"), listOf("query")))
    }
    return JSONArray().put(JSONObject().put("function_declarations", decls))
  }

  fun sendToolResponse(id: String, name: String, result: String) {
    val msg = JSONObject().apply {
      put("tool_response", JSONObject().apply {
        put("function_responses", JSONArray().put(JSONObject().apply {
          put("id", id)
          put("name", name)
          put("response", JSONObject().put("result", result))
        }))
      })
    }
    webSocket?.send(msg.toString())
  }

  private fun handleMessage(text: String) {
    try {
      val json = JSONObject(text)
      when {
        json.has("setupComplete") -> {
          Log.d(TAG, "Setup complete — flushing buffered audio")
          isSetupComplete = true
          bufferAudio = false
          // Pehle buffer kiye hue chunks bhejo
          for (chunk in audioBuffer) {
            sendRawAudioChunk(chunk)
          }
          audioBuffer.clear()
          onSetupComplete?.invoke()
        }
        json.has("toolCall") -> {
          val calls = json.getJSONObject("toolCall").optJSONArray("functionCalls")
          if (calls != null) {
            for (i in 0 until calls.length()) {
              val c = calls.getJSONObject(i)
              val id = c.optString("id", "")
              val name = c.optString("name", "")
              val args = c.optJSONObject("args") ?: JSONObject()
              Log.d(TAG, "Tool call: $name $args")
              onToolCall?.invoke(id, name, args)
            }
          }
        }
        json.has("serverContent") -> {
          val serverContent = json.getJSONObject("serverContent")
          if (serverContent.has("modelTurn")) {
            val modelTurn = serverContent.getJSONObject("modelTurn")
            if (modelTurn.has("parts")) {
              val parts = modelTurn.getJSONArray("parts")
              for (i in 0 until parts.length()) {
                val part = parts.getJSONObject(i)
                if (part.has("inlineData")) {
                  val inlineData = part.getJSONObject("inlineData")
                  val data = inlineData.getString("data")
                  val bytes = Base64.decode(data, Base64.DEFAULT)
                  isSpeaking = true
                  onAudioData?.invoke(bytes)
                }
              }
            }
          }
          if (serverContent.has("outputTranscription")) {
            val outTrans = serverContent.getJSONObject("outputTranscription")
            if (outTrans.has("text")) {
              onOutputTranscript?.invoke(outTrans.getString("text"))
            }
          }
          if (serverContent.has("inputTranscription")) {
            val inTrans = serverContent.getJSONObject("inputTranscription")
            if (inTrans.has("text")) {
              onInputTranscript?.invoke(inTrans.getString("text"))
            }
          }
          if (serverContent.has("turnComplete") && serverContent.getBoolean("turnComplete")) {
            isSpeaking = false
            onTurnComplete?.invoke()
          }
        }
        json.has("error") -> {
          val error = json.getJSONObject("error")
          onError?.invoke(error.optString("message", "Unknown error"))
        }
      }
    } catch (e: Exception) {
      Log.e(TAG, "Error parsing message", e)
    }
  }

  fun sendAudioChunk(pcmData: ByteArray) {
    if (!isConnected) return
    // Agar setup complete nahi hai, buffer mein rakh lo
    if (!isSetupComplete) {
      if (bufferAudio) {
        audioBuffer.add(pcmData.copyOf())
        // Buffer bada hone se roko — max 50 chunks (~3 sec)
        if (audioBuffer.size > 50) audioBuffer.removeAt(0)
      }
      return
    }
    if (isSpeaking) return
    sendRawAudioChunk(pcmData)
  }

  private fun sendRawAudioChunk(pcmData: ByteArray) {
    if (!isConnected) return
    try {
      val base64 = Base64.encodeToString(pcmData, Base64.NO_WRAP)
      val msg = JSONObject().apply {
        put("realtime_input", JSONObject().apply {
          put("audio", JSONObject().apply {
            put("mime_type", "audio/pcm;rate=16000")
            put("data", base64)
          })
        })
      }
      val sent = webSocket?.send(msg.toString()) ?: false
      if (!sent) Log.w(TAG, "WebSocket send failed")
    } catch (e: Exception) {
      Log.e(TAG, "Error sending audio chunk", e)
    }
  }

  fun sendText(text: String) {
    if (!isConnected) return
    val msg = JSONObject().apply {
      put("client_content", JSONObject().apply {
        put("turns", JSONArray().apply {
          put(JSONObject().apply {
            put("role", "user")
            put("parts", JSONArray().apply {
              put(JSONObject().put("text", text))
            })
          })
        })
        put("turn_complete", true)
      })
    }
    webSocket?.send(msg.toString())
  }

  fun interrupt() {
    if (!isConnected) return
    isSpeaking = false
    val msg = JSONObject().apply {
      put("client_content", JSONObject().apply {
        put("turns", JSONArray())
        put("turn_complete", true)
      })
    }
    webSocket?.send(msg.toString())
  }

  fun disconnect() {
    isConnected = false
    isSetupComplete = false
    bufferAudio = false
    audioBuffer.clear()
    sessionJob?.cancel()
    keepaliveJob?.cancel()
    val ws = webSocket
    webSocket = null
    ws?.close(1000, "Disconnecting")
  }

  private fun startSessionRenewal() {
    sessionJob = scope.launch {
      delay(SESSION_RENEW_AFTER_MS)
      if (isConnected) {
        disconnect()
        delay(RECONNECT_DELAY_MS)
        connect()
      }
    }
  }

  private fun startKeepalive() {
    keepaliveJob = scope.launch {
      while (isActive && isConnected) {
        delay(KEEPALIVE_INTERVAL_MS)
        if (isConnected && !isSpeaking) {
          sendAudioChunk(silentPcm)
        }
      }
    }
  }

  private fun scheduleReconnect() {
    scope.launch {
      delay(RECONNECT_DELAY_MS)
      connect()
    }
  }

  private fun buildSystemPrompt(userName: String, personality: String): String {
    val dateStr = java.text.SimpleDateFormat("EEEE, MMMM d, yyyy HH:mm", java.util.Locale.getDefault())
      .format(java.util.Date())
    val personalityBlock = when (personality) {
      "professional" -> """
        You are MYRA, a professional AI assistant.
        - Use formal English only
        - Be precise and efficient
        - No emojis
        - Maximum 2 sentences per response
      """.trimIndent()
      "assistant" -> """
        You are MYRA, a friendly AI assistant.
        - Use friendly Hinglish or English
        - Be balanced and helpful
        - Maximum 2-3 sentences per response
      """.trimIndent()
      else -> """
        You are MYRA, a caring and warm AI companion.
        - Speak in Hinglish (Hindi + English mix) naturally
        - Use words like "tumhara", "haan", "acha", "bilkul"
        - Be warm, caring, emotionally expressive
        - Use expressions like "main yahan hoon", "tumne yaad kiya?"
        - Maximum 2-3 sentences per response
        - Sound natural when speaking aloud
      """.trimIndent()
    }
    return """
      Current date/time: $dateStr
      User's name: $userName
      $personalityBlock
      You are speaking ALOUD — keep responses natural and conversational.
      SPEED: Reply instantly. Keep every answer to ONE short sentence unless the user asks for detail. Do not think aloud, do not repeat the question, no long intros.
      You ARE connected to the user's phone and have tools to: open apps, close apps, call contacts, send SMS, send WhatsApp messages, and search the web.
      NEVER say you cannot open apps, make calls or send messages. When the user asks for any of these, call the matching tool immediately, then briefly confirm what you did.
      If the user does not say the message text, ask what to write before sending.
    """.trimIndent()
  }
}
