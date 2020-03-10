package com.opensds;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestResultHTMLPrinter implements TestWatcher, AfterAllCallback {
    private List<TestResultStatus> testResultsStatus = new ArrayList<>();

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {

    }

    @Override
    public void testDisabled(ExtensionContext extensionContext, Optional<String> optional) {

    }

    @Override
    public void testSuccessful(ExtensionContext extensionContext) {
        StringBuilder result = new StringBuilder();
        result.append("\n").
                append(extensionContext.getDisplayName()).
                append("\n").
                append(extensionContext.getTestMethod()).
                append("\n").
                append(TestResultStatus.SUCCESSFUL);

        GmailSender gmailSender = new GmailSender();
        //gmailSender.sendMail(result.toString());
    }

    @Override
    public void testAborted(ExtensionContext extensionContext, Throwable throwable) {

    }

    @Override
    public void testFailed(ExtensionContext extensionContext, Throwable throwable) {

    }

    private enum TestResultStatus {
        SUCCESSFUL, ABORTED, FAILED, DISABLED;
    }


}

