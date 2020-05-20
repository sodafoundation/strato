package com.opensds;

import com.opensds.jsonmodels.akskresponses.SignatureKey;
import com.opensds.jsonmodels.inputs.createbucket.CreateBucketFileInput;
import com.opensds.jsonmodels.projectsresponses.ProjectsHolder;
import com.opensds.jsonmodels.tokensresponses.TokenHolder;
import com.opensds.jsonmodels.typesresponse.TypesHolder;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Main {

    public static String getSha256Hex(String text, String encoding) {
        String shaHex = "";
        try {
            MessageDigest md = null;

            md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes(encoding));
            byte[] digest = md.digest();

            shaHex = DatatypeConverter.printHexBinary(digest);


        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            System.out.println(ex);
        }
        return shaHex.toLowerCase();
    }


    private static String getHmacSHA256(String message, String secret) {
        Mac sha256_HMAC = null;
        String hash = null;
        try {
            sha256_HMAC = Mac.getInstance("HmacSHA256");

            SecretKeySpec secret_key = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secret_key);

            //hash = Base64.getEncoder().encodeToString(sha256_HMAC.doFinal(message.getBytes()));
            hash = Hex.encodeHexString(sha256_HMAC.doFinal(message.getBytes()));

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
        }

        return hash;
    }

    public static void main(String[] args) {

        //System.out.println(getSha256Hex("Message", "UTF-8"));
        System.out.println(getHmacSHA256("Message", "secret"));

    }

    /*public static void main(String[] args) {

        // write your code here
        HttpHandler httpHandler = new HttpHandler();

        TokenHolder tokenHolder = httpHandler.loginAndGetToken();


        ProjectsHolder projectsHolder = httpHandler.getProjects(tokenHolder.getResponseHeaderSubjectToken(), tokenHolder.getToken().getUser().getId());


        com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder authTokenHolder = httpHandler.getAuthToken(tokenHolder.getResponseHeaderSubjectToken());

        TypesHolder typesHolder = httpHandler.getTypes(authTokenHolder.getResponseHeaderSubjectToken(), authTokenHolder.getToken().getProject().getId());


        *//*int code = httpHandler.addBackend(authTokenHolder.getResponseHeaderSubjectToken(),
                authTokenHolder.getToken().getProject().getId(),
                );*//*
        //System.out.println(code);

        SignatureKey signatureKey = httpHandler.getAkSkList(authTokenHolder.getResponseHeaderSubjectToken(),
                authTokenHolder.getToken().getProject().getId());


    }*/

    /*public static void main(String[] args) {

        try {
            File dir = new File("/root/javaproj/osdsjunit/inputs/addbackend");
            File [] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.matches("^aws-s3+[a-z_1-9-]*.json");
                }
            });

            for (File xmlfile : files) {
                System.out.println(xmlfile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/


}
