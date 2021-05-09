package com.nokia.web.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import com.nokia.ca4ci.crypto.client.CryptoClient;
import com.nokia.web.util.ConfigUtil;

public class JobMonitorWSClient {
	Logger logger = Logger.getLogger("nokia");
	CryptoClient cryptoClient = new CryptoClient();
	
	RestTemplate restTemplate;
	public RestTemplate getRestClient() throws KeyManagementException, UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException{
		RestTemplate restTemplate = new RestTemplate(getRequestFactory());
		return restTemplate;
	}
	
	private ClientHttpRequestFactory getRequestFactory() throws KeyStoreException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, KeyManagementException, UnrecoverableKeyException {
		
		String keyStorePassword = cryptoClient.decrypt(ConfigUtil.getConfig().getProperty("Secret"));
		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		keyStore.load(new FileInputStream(FileSystems.getDefault().getPath(ConfigUtil.getConfig().getProperty("KeystoreFile")).normalize().toFile()),
				keyStorePassword.toCharArray());
		
		SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(
				new SSLContextBuilder()
						.loadTrustMaterial(null, new TrustSelfSignedStrategy())
						.loadKeyMaterial(keyStore,
								keyStorePassword.toCharArray()).build(),
				NoopHostnameVerifier.INSTANCE);
		HttpClient httpClient = HttpClients.custom()
				.setSSLSocketFactory(socketFactory).build();
		ClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(
				httpClient);
		return requestFactory;
	}
		
}
