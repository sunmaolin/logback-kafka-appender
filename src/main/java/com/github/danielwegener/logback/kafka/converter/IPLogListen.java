package com.github.danielwegener.logback.kafka.converter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;

import java.util.logging.LogManager;


public class IPLogListen implements LoggerContextListener, StatusListener {
    @Override
    public boolean isResetResistant() {
        System.out.println("123456isResetResistant\n");
        return false;
    }

    @Override
    public void onStart(LoggerContext context) {
        System.out.println("123456onStart\n" + context.getCopyOfPropertyMap() + "123456onStart\n");
    }

    @Override
    public void onReset(LoggerContext context) {
        System.out.println("123456onReset\n" + context.getCopyOfPropertyMap() + "123456onReset\n");
    }

    @Override
    public void onStop(LoggerContext context) {
        System.out.println("123456onStop\n" + context.getCopyOfPropertyMap() + "123456onStop\n");
    }

    @Override
    public void onLevelChange(Logger logger, Level level) {
        System.out.println("123456onLevelChange\n" + level.toInt() + "123456onLevelChange\n");
    }

    @Override
    public void addStatusEvent(Status status) {
        System.out.println("123456addStatusEvent\n" + status.toString() + "123456addStatusEvent\n"+LogManager.getLogManager().getProperty("PROJECT_NAME"));
    }
}
