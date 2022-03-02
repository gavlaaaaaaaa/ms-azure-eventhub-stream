package com.microsoft.streaming.eventhubstream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Base64.Encoder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketFactory;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class EventhubstreamApplication {

	private static String URL = "wss://ws.blockchain.info/mercury-gateway/v1/ws";
	private static String eventHubKey = "<your event hub key here>";

	public static void main(String[] args) throws IOException {
		Map<String, String> env = System.getenv();
		String authToken = env.get("blockchainAuthToken");

		// Create a websocket with the blockchain websocket URL
		WebSocket ws = new WebSocketFactory().createSocket(URL);
		// Add required header for connection
		ws.addHeader("Origin","https://exchange.blockchain.com");
		// add a listener to process a message when it is sent to the websocket
		ws.addListener(new WebSocketAdapter() {
			String auth = GetSASToken("msblog.servicebus.windows.net", "java-app", eventHubKey);
			String URL = "https://msblog.servicebus.windows.net/blockchain-usd-btc-price/messages?timeout=60&api-version=2014-01";
			// create an array to hold the batch
			JSONArray array = new JSONArray();

			@Override
			public void onTextMessage(WebSocket websocket, String message) throws Exception {
				// Received a text message so lets convert it to a json object and store in an array
				System.out.println(message);
				JSONObject obj = new JSONObject();
				obj.put("Body", message);
				array.add(obj);

				if (array.size() > 20) {
					// if the batch is over 100 messages, send it and then create a new batch
					CloseableHttpClient httpClient = HttpClientBuilder.create().build();
					try {
						HttpPost request = new HttpPost(URL);
						request.setHeader("Content-Type", "application/vnd.microsoft.servicebus.json");
						request.setHeader("Authorization", auth);
						request.setEntity(new StringEntity(array.toJSONString()));
						CloseableHttpResponse response = httpClient.execute(request);
						response.close();
					}
					finally {
						httpClient.close();
					}
					// reset array
					array = new JSONArray();
				}
			}
		});

		try {
			ws.connect();
			// subscribe to the prices channel for USD-BTC price data
			ws.sendText("{\"token\": \""+authToken+"\", \"action\": \"subscribe\", \"channel\": \"prices\", \"symbol\": \"BTC-USD\",\"granularity\": 60}");
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
			ws.disconnect();
		}

	}

	private static String GetSASToken(String resourceUri, String keyName, String key)
  {
      long epoch = System.currentTimeMillis()/1000L;
      int week = 60*60*24*7;
      String expiry = Long.toString(epoch + week);

      String sasToken = null;
      try {
          String stringToSign = URLEncoder.encode(resourceUri, "UTF-8") + "\n" + expiry;
          String signature = getHMAC256(key, stringToSign);
          sasToken = "SharedAccessSignature sr=" + URLEncoder.encode(resourceUri, "UTF-8") +"&sig=" +
                  URLEncoder.encode(signature, "UTF-8") + "&se=" + expiry + "&skn=" + keyName;
      } catch (UnsupportedEncodingException e) {

          e.printStackTrace();
      }

      return sasToken;
  }


public static String getHMAC256(String key, String input) {
    Mac sha256_HMAC = null;
    String hash = null;
    try {
        sha256_HMAC = Mac.getInstance("HmacSHA256");
        SecretKeySpec secret_key = new SecretKeySpec(key.getBytes(), "HmacSHA256");
        sha256_HMAC.init(secret_key);
        Encoder encoder = Base64.getEncoder();

        hash = new String(encoder.encode(sha256_HMAC.doFinal(input.getBytes("UTF-8"))));

    } catch (InvalidKeyException e) {
        e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
    } catch (IllegalStateException e) {
        e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
    }

    return hash;
}

}
