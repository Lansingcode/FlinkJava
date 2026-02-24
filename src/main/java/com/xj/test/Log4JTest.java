package com.xj.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4JTest {
    private static final Logger logger = LoggerFactory.getLogger(Log4JTest.class);

    public static void main(String[] args) {
        // 记录debug级别的信息
        logger.debug("This is debug message.");
        // 记录info级别的信息
        logger.info("This is info message.");
        // 记录error级别的信息
        logger.error("This is error message.");
    }
}
