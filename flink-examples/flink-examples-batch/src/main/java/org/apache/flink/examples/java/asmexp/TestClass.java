package org.apache.flink.examples.java.asmexp;

public class TestClass {

    TestClass f;

    final int x;


    public TestClass(Integer x) {
        //this.f = f;
        this.x = x;
    }


    @Override
    public String toString() {
        return "TestClass{" +
                "f=" + f +
                ", x=" + x +
                '}';
    }
}
