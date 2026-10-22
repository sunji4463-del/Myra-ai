package com.myra.assistant.ui.main

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.speech.RecognitionListener
import android.speech.RecognizerIntent
import android.speech.SpeechRecognizer
import android.util.Log
import android.view.View
import android.view.animation.AlphaAnimation
import android.widget.ImageButton
import android.widget.TextView
import android.widget.Toast
import androidx.activity.viewModels
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.myra.assistant.BuildConfig
import com.myra.assistant.R
import com.myra.assistant.ai.AudioEngine
import com.myra.assistant.ai.CommandParser
import com.myra.assistant.ai.GeminiLiveClient
import com.myra.assistant.model.ChatMessage
import com.myra.assistant.service.CallMonitorService
import com.myra.assistant.ui.settings.SettingsActivity
import com.myra.assistant.viewmodel.MainViewModel

class MainActivity : AppCompatActivity() {

  companion object {
    const val TAG = "MainActivity"
    const val PERMISSIONS_REQUEST_CODE = 100
    const val CALL_STT_TIMEOUT_MS = 4500L
  }

  private val viewModel: MainViewModel by viewModels()
  private lateinit var prefs: android.content.SharedPreferences
  private lateinit var geminiLive: GeminiLiveClient
  private lateinit var audioEngine: AudioEngine
  private lateinit var chatAdapter: ChatAdapter

  private lateinit var orbView: OrbAnimationView
  private lateinit var waveformView: WaveformView
  private lateinit var statusText: TextView
  private lateinit var micButton: ImageButton
  private lateinit var settingsBtn: ImageButton
  private lateinit var chatRecycler: RecyclerView
  private lateinit var redOverlay: View
  private lateinit var batteryText: TextView
  private lateinit var ramText: TextView
  private lateinit var timeText: TextView

  private val inputBuffer = StringBuilder()
  private val outputBuffer = StringBuilder()
  private var isInCallMode = false
  private var isActive = false
  private var isMuted = false

  private var callStt: SpeechRecognizer? = null
  private val handler = Handler(Looper.getMainLooper())
  private var receiverRegistered = false

  private val callEndedReceiver = object : BroadcastReceiver() {
    override fun onReceive(context: Context?, intent: Intent?) {
      try {
        if (intent?.action == "com.myra.CALL_ENDED") {
          isInCallMode = false
          setActiveMode(false)
        }
      } catch (e: Exception) {
        Log.e(TAG, "Error in call ended receiver", e)
      }
    }
  }

  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    try {
      setContentView(R.layout.activity_main)
    } catch (e: Exception) {
      Log.e(TAG, "Error setting content view", e)
      Toast.makeText(this, "Error loading UI", Toast.LENGTH_LONG).show()
      finish()
      return
    }

    prefs = getSharedPreferences("myra_prefs", Context.MODE_PRIVATE)

    try
