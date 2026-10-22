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
            live.add(m.getString("name"))
            break
          }
        }
      }
      Log.d(TAG, "Live models: $live")
      when {
        live.isEmpty() -> fallback
        saved != null && live.contains(saved) -> saved
        else -> live.firstOrNull { it.contains("native-audio") && !it.contains("preview-09") }
          ?: live.firstOrNull { it.contains("native-audio") }
          ?: live.firstOrNull { it.contains("live") }
          ?: live.first()
      }
    } catch (e: Exception) {
      Log.e(TAG, "resolveModel failed", e)
      fallback
    }
  }

  private fun openSocket(apiKey: String, model: String) {
    val voice = prefs.getString("gemini_voice", "Aoede") ?: "Aoede"
    val userName = prefs.getString("user_name", "Sir") ?: "Sir"
    val personality = prefs.getString("personality_mode", "gf") ?: "gf"

    val systemPrompt = buildSystemPrompt(userName, personality)

    val wsUrl = "ws
