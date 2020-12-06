package com.opensds;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class GmailSender {
    public static void main(String[] args) {
        GmailSender sender = new GmailSender();
        sender.sendMail("yadda");
    }

    public void sendMail(String body) {

        final String username = "";
        final String password = "";

        Properties prop = new Properties();
        prop.put("mail.smtp.host", "smtp.gmail.com");
        prop.put("mail.smtp.port", "587");
        prop.put("mail.smtp.auth", "true");
        prop.put("mail.smtp.starttls.enable", "true"); //TLS

        Session session = Session.getInstance(prop,
                new javax.mail.Authenticator() {
                    protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("rhsakarpos@gmail.com"));
            message.setRecipients(
                    Message.RecipientType.TO,
                    InternetAddress.parse("1@gmail.com," +
                            "2@gmail.com," +
                            "3@gmail.com," +
                            "4@gmail.com"
                    )

            );
            message.setSubject("OpenSDS multi-cloud API smoke test");
            message.setText(body);

            Transport.send(message);

        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

}
