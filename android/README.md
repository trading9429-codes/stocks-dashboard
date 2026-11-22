# 📱 Stock Scanner Android App

Native Android wrapper with full hardware acceleration, Android 13+ background push alerts, swipe-to-refresh, tactile vibrations, and 1-click broker execution.

---

## 🚀 Features

* **Real-time Mobile Trading Cards**: Custom UI/UX designed for 1-hand mobile operation.
* **Native Android Notifications**: Fires instant system notifications with a dedicated High Importance channel (`stock_scanner_alerts`) and vibration patterns.
* **Android 13+ Ready**: Automatically handles `POST_NOTIFICATIONS` runtime permissions.
* **Swipe-to-Refresh**: Native pull-down to refresh alerts.
* **Audio Autoplay**: Unlocked Web Audio so alert chimes trigger without requiring manual gestures.
* **Zero Lag WebView**: Configured with hardware acceleration, DomStorage, and WebGL for TradingView charts.
* **Smart Back Button**: Closes bottom sheet modals and sizers before exiting.

---

## 🛠️ How to Open & Build in Android Studio

1. **Open Project**:
   * Open Android Studio.
   * Select **Open** and choose the `android` folder in this repository:
     `C:\Users\Sunny PC\.gemini\antigravity\scratch\stocks-dashboard\android`
2. **Configure Target URL**:
   * Open `android/app/src/main/res/values/strings.xml`.
   * Update `default_dashboard_url` to your live deployed server URL (e.g. `https://imbalance-stocks-dashboard.onrender.com` or local LAN IP like `http://192.168.1.X:5000` for testing).
3. **Build & Run**:
   * Connect your Android phone via USB (with USB Debugging enabled) or start an Android Emulator.
   * Click **Run** (`Shift + F10`) or select **Build > Build Bundle(s) / APK(s) > Build APK(s)** to generate `app-debug.apk`.
