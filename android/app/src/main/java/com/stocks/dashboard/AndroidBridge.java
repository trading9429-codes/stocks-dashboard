package com.stocks.dashboard;

import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.os.VibrationEffect;
import android.os.Vibrator;
import android.webkit.JavascriptInterface;
import android.widget.Toast;

import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;

public class AndroidBridge {
    public static final String CHANNEL_ID = "stock_scanner_alerts";
    private final MainActivity activity;

    public AndroidBridge(MainActivity activity) {
        this.activity = activity;
        createNotificationChannel();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            CharSequence name = activity.getString(R.string.notification_channel_name);
            String description = activity.getString(R.string.notification_channel_desc);
            int importance = NotificationManager.IMPORTANCE_HIGH;
            NotificationChannel channel = new NotificationChannel(CHANNEL_ID, name, importance);
            channel.setDescription(description);
            channel.enableVibration(true);
            channel.setVibrationPattern(new long[]{0, 250, 80, 250, 80, 250});

            NotificationManager notificationManager = activity.getSystemService(NotificationManager.class);
            if (notificationManager != null) {
                notificationManager.createNotificationChannel(channel);
            }
        }
    }

    @JavascriptInterface
    public boolean isAndroidApp() {
        return true;
    }

    @JavascriptInterface
    public void showNotification(String title, String message, String stock, String price, double relVol) {
        try {
            Intent intent = new Intent(activity, MainActivity.class);
            intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
            intent.putExtra("stock", stock);
            intent.putExtra("price", price);

            PendingIntent pendingIntent = PendingIntent.getActivity(
                    activity,
                    (int) System.currentTimeMillis(),
                    intent,
                    PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0)
            );

            // Action to open TradingView directly
            Intent tvIntent = new Intent(Intent.ACTION_VIEW, Uri.parse("https://in.tradingview.com/chart/?symbol=NSE:" + Uri.encode(stock)));
            PendingIntent tvPendingIntent = PendingIntent.getActivity(
                    activity,
                    (int) System.currentTimeMillis() + 1,
                    tvIntent,
                    PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0)
            );

            long[] vibPattern = relVol >= 2.0
                    ? new long[]{0, 300, 80, 300, 80, 300}
                    : new long[]{0, 150, 80, 150};

            NotificationCompat.Builder builder = new NotificationCompat.Builder(activity, CHANNEL_ID)
                    .setSmallIcon(R.drawable.ic_notification)
                    .setContentTitle(title)
                    .setContentText(message)
                    .setStyle(new NotificationCompat.BigTextStyle().bigText(message))
                    .setPriority(NotificationCompat.PRIORITY_HIGH)
                    .setDefaults(NotificationCompat.DEFAULT_SOUND)
                    .setVibrate(vibPattern)
                    .setAutoCancel(true)
                    .setContentIntent(pendingIntent)
                    .addAction(R.drawable.ic_notification, "📊 5m Chart", tvPendingIntent);

            NotificationManagerCompat notificationManager = NotificationManagerCompat.from(activity);
            int notifId = stock.hashCode();
            notificationManager.notify(notifId, builder.build());
        } catch (SecurityException e) {
            // Android 13+ permission not granted
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @JavascriptInterface
    public void vibrate(int durationMs) {
        try {
            Vibrator v = (Vibrator) activity.getSystemService(Context.VIBRATOR_SERVICE);
            if (v != null && v.hasVibrator()) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    v.vibrate(VibrationEffect.createOneShot(durationMs, VibrationEffect.DEFAULT_AMPLITUDE));
                } else {
                    v.vibrate(durationMs);
                }
            }
        } catch (Exception ignored) {}
    }

    @JavascriptInterface
    public void copyToClipboard(String label, String text) {
        activity.runOnUiThread(() -> {
            try {
                ClipboardManager clipboard = (ClipboardManager) activity.getSystemService(Context.CLIPBOARD_SERVICE);
                ClipData clip = ClipData.newPlainText(label, text);
                if (clipboard != null) {
                    clipboard.setPrimaryClip(clip);
                    Toast.makeText(activity, "Copied: " + text, Toast.LENGTH_SHORT).show();
                }
            } catch (Exception ignored) {}
        });
    }

    @JavascriptInterface
    public void openExternalUrl(String url) {
        activity.runOnUiThread(() -> {
            try {
                Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
                activity.startActivity(intent);
            } catch (Exception ignored) {}
        });
    }
}
