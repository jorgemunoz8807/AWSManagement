/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.main;

/**
 *
 * @author jmunoz
 */
public class test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

       
        new Thread(() -> method1()).start();
        new Thread(() -> method2()).start();
    }

    public static void method1() {
        while (true) {
            System.out.println("Jorge");
        }
    }

    public static void method2() {
        while (true) {
            System.out.println("Munoz");
        }
    }

}
