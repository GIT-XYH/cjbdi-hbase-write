package com.cjbdi.version3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: XYH
 * @Date: 2021/11/18 10:03 上午
 * @Description: 多线程测试
 */
public class ThreadTest {
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 100; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName()
                    );
                }
            });
        }
        pool.shutdown();
        for (int j = 0; j < 100; j++) {
            System.out.println("这是主线程" + Thread.currentThread().getName());
        }
    }
}
