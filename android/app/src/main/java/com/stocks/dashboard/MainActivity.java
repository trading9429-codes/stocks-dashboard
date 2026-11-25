package com.stocks.dashboard;

import android.Manifest;
import android.annotation.SuppressLint;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.Toast;

import androidx.activity.OnBackPressedCallback;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import androidx.swiperefreshlayout.widget.SwipeRefreshLayout;

import com.google.android.material.button.MaterialButton;

public class MainActivity extends AppCompatActivity {

    private static final int PERMISSION_REQUEST_NOTIFICATIONS = 101;
    private WebView webView;
    private SwipeRefreshLayout swipeRefresh;
    private ProgressBar progressBar;
    private LinearLayout loadingOverlay;
    private LinearLayout errorOverlay;
    private boolean hasMainFrameError = false;

    @SuppressLint("SetJavaScriptEnabled")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        webView = findViewById(R.id.webview);
        swipeRefresh = findViewById(R.id.swipe_refresh);
        progressBar = findViewById(R.id.progress_bar);
        loadingOverlay = findViewById(R.id.loading_overlay);
        errorOverlay = findViewById(R.id.error_overlay);
        MaterialButton btnRetry = findViewById(R.id.btn_retry);

        btnRetry.setOnClickListener(v -> {
            hasMainFrameError = false;
            errorOverlay.setVisibility(View.GONE);
            loadingOverlay.setAlpha(1f);
            loadingOverlay.setVisibility(View.VISIBLE);
            webView.clearCache(true);
            webView.reload();
        });

        // Configure SwipeRefreshLayout (only pull-to-refresh when WebView is at top)
        swipeRefresh.setColorSchemeResources(R.color.accent_green, R.color.accent_blue);
        swipeRefresh.setProgressBackgroundColorSchemeResource(R.color.surface_dark);
        swipeRefresh.setOnChildScrollUpCallback((parent, child) -> webView.getScrollY() > 0);
        swipeRefresh.setOnRefreshListener(() -> {
            hasMainFrameError = false;
            errorOverlay.setVisibility(View.GONE);
            webView.clearCache(true);
            webView.reload();
        });

        // Configure WebView
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setDatabaseEnabled(true);
        settings.setAllowFileAccess(true);
        settings.setLoadWithOverviewMode(true);
        settings.setUseWideViewPort(true);
        settings.setSupportZoom(false);
        settings.setBuiltInZoomControls(false);
        settings.setDisplayZoomControls(false);
        settings.setCacheMode(WebSettings.LOAD_NO_CACHE);
        settings.setMediaPlaybackRequiresUserGesture(false); // Enable auto alert chime
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);

        // Clear stale disk cache so user immediately receives latest UI updates
        webView.clearCache(true);

        // Append custom user agent identifier
        String defaultUA = settings.getUserAgentString();
        settings.setUserAgentString(defaultUA + " StocksAndroidApp/2.1");

        // Add JavaScript Bridge
        AndroidBridge bridge = new AndroidBridge(this);
        webView.addJavascriptInterface(bridge, "Android");

        webView.setWebViewClient(new WebViewClient() {
            @Override
            public void onPageStarted(WebView view, String url, Bitmap favicon) {
                super.onPageStarted(view, url, favicon);
                hasMainFrameError = false;
                progressBar.setVisibility(View.VISIBLE);
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                super.onPageFinished(view, url);
                progressBar.setVisibility(View.GONE);
                swipeRefresh.setRefreshing(false);
                if (!hasMainFrameError && loadingOverlay.getVisibility() == View.VISIBLE) {
                    loadingOverlay.animate()
                            .alpha(0f)
                            .setDuration(220)
                            .withEndAction(() -> loadingOverlay.setVisibility(View.GONE))
                            .start();
                }
            }

            @Override
            public void onReceivedError(WebView view, WebResourceRequest request, WebResourceError error) {
                super.onReceivedError(view, request, error);
                if (request.isForMainFrame()) {
                    hasMainFrameError = true;
                    progressBar.setVisibility(View.GONE);
                    swipeRefresh.setRefreshing(false);
                    loadingOverlay.setVisibility(View.GONE);
                    errorOverlay.setVisibility(View.VISIBLE);
                }
            }

            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                String url = request.getUrl().toString();
                // Keep dashboard routes inside WebView, open external broker & charting sites in native app/browser
                if (url.contains("tradingview.com") || url.contains("fyers.in") || url.contains("dhan.co") || url.contains("chartink.com")) {
                    try {
                        Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
                        startActivity(intent);
                        return true;
                    } catch (Exception ignored) {}
                }
                return false;
            }

            @SuppressWarnings("deprecation")
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, String url) {
                if (url.contains("tradingview.com") || url.contains("fyers.in") || url.contains("dhan.co") || url.contains("chartink.com")) {
                    try {
                        Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
                        startActivity(intent);
                        return true;
                    } catch (Exception ignored) {}
                }
                return false;
            }
        });

        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public void onProgressChanged(WebView view, int newProgress) {
                if (newProgress < 100) {
                    progressBar.setVisibility(View.VISIBLE);
                    progressBar.setProgress(newProgress);
                } else {
                    progressBar.setVisibility(View.GONE);
                }
            }
        });

        // Handle Back button
        getOnBackPressedDispatcher().addCallback(this, new OnBackPressedCallback(true) {
            @Override
            public void handleOnBackPressed() {
                // Check if web modal is open first via JavaScript
                webView.evaluateJavascript("(function(){ const m = document.querySelector('.modal-overlay:not(.hidden)'); if (m) { m.classList.add('hidden'); return true; } return false; })()", value -> {
                    if ("true".equals(value)) {
                        return;
                    }
                    if (webView.canGoBack()) {
                        webView.goBack();
                    } else {
                        finish();
                    }
                });
            }
        });

        // Request Android 13+ Notification Permission
        checkNotificationPermission();

        // Load Dashboard URL
        String targetUrl = getString(R.string.default_dashboard_url);
        handleIntent(getIntent(), targetUrl);
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        handleIntent(intent, null);
    }

    private void handleIntent(Intent intent, String fallbackUrl) {
        if (intent != null && intent.hasExtra("stock")) {
            String stock = intent.getStringExtra("stock");
            String price = intent.getStringExtra("price");
            // Highlight or open position sizer directly for clicked notification
            String script = String.format("if (typeof openPositionSizer === 'function') { openPositionSizer('%s', '%s'); }", stock, price);
            webView.evaluateJavascript(script, null);
        } else if (fallbackUrl != null) {
            webView.loadUrl(fallbackUrl);
        }
    }

    private void checkNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.POST_NOTIFICATIONS}, PERMISSION_REQUEST_NOTIFICATIONS);
            }
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == PERMISSION_REQUEST_NOTIFICATIONS) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                Toast.makeText(this, "🔔 Scanner Alerts Enabled!", Toast.LENGTH_SHORT).show();
            }
        }
    }
}
