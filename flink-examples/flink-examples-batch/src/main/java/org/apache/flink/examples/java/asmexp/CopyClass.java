package org.apache.flink.examples.java.asmexp;

import org.apache.flink.util.SpecUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.SimpleRemapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;

public class CopyClass {

    public static void main(String[] args) throws Exception {


//        ClassReader reader = new ClassReader("org.apache.flink.examples.java.asmexp.TestClass");
//
//        ClassWriter writer = new ClassWriter(reader, 0);
//
//        //ClassVisitor visitor = new ChangeNameAdapter(writer);
//        //ClassVisitor visitor = new ClassRemapper(new ChangeNameAdapter(writer), new SimpleRemapper("org.apache.flink.examples.java.asmexp.TestClass", "org.apache.flink.examples.java.asmexp.TestClassRenamed"));
//        //ClassVisitor visitor = new ClassRemapper(writer, new SimpleRemapper("org.apache.flink.examples.java.asmexp.TestClass", "org.apache.flink.examples.java.asmexp.TestClassRenamed"));
//        ClassVisitor visitor = new ClassRemapper(writer, new SimpleRemapper("org/apache/flink/examples/java/asmexp/TestClass", "org/apache/flink/examples/java/asmexp/TestClassRenamed"));
//
//        reader.accept(visitor, 0);
//
//        Class copied = loadClass("org.apache.flink.examples.java.asmexp.TestClassRenamed", writer.toByteArray());
//
//        //Object copiedInst = copied.newInstance();
//        //Object copiedInst = InstantiationUtil.instantiate(copied);
//        Object copiedInst = instantiate(copied, 5);


        //Object copiedInst = copyClassAndInstantiate("org.apache.flink.examples.java.asmexp.TestClass", 5);

        Object copiedInst = SpecUtil.copyClassAndInstantiate("org.apache.flink.examples.java.asmexp.TestClass", 5);

        System.out.println(copiedInst.toString());



    }

//
//    private static final Map<String, Integer> copyCounts = new TreeMap<>();
//
//    private static Object copyClassAndInstantiate(String name, Object... ctorArgs) throws Exception {
//        ClassReader reader = new ClassReader(name);
//        ClassWriter writer = new ClassWriter(reader, 0);
//
//        int oldCount = copyCounts.getOrDefault(name, 0);
//        String newName = name + "__copy_" + oldCount;
//        copyCounts.put(name, oldCount + 1);
//
//        String nameWithSlashes = name.replace('.', '/');
//        String newNameWithSlashes = newName.replace('.', '/');
//        ClassVisitor visitor = new ClassRemapper(writer, new SimpleRemapper(nameWithSlashes, newNameWithSlashes));
//
//        reader.accept(visitor, 0);
//
//        Class copied = loadClass(newName, writer.toByteArray());
//
//        return instantiate(copied, ctorArgs);
//    }
//
//    // automatically finds the appropriate ctor based on the given arguments' types
//    private static Object instantiate(Class clazz, Object... args) throws IllegalAccessException, InvocationTargetException, InstantiationException {
//        Class[] ctorParamTypes = new Class[args.length];
//        int i=0;
//        for (Object arg: args) {
//            ctorParamTypes[i++] = arg.getClass();
//        }
//        Constructor ctor;
//        try {
//            ctor = clazz.getConstructor(ctorParamTypes);
//        } catch (NoSuchMethodException e) {
//            throw new RuntimeException(e);
//        }
//        return ctor.newInstance(args);
//    }
//
//
//    // copy-paste from https://asm.ow2.io/faq.html#Q5
//    private static Class loadClass(String className, byte[] b) {
//        // Override defineClass (as it is protected) and define the class.
//        Class clazz = null;
//        try {
//            ClassLoader loader = ClassLoader.getSystemClassLoader();
//            Class cls = Class.forName("java.lang.ClassLoader");
//            java.lang.reflect.Method method =
//                    cls.getDeclaredMethod(
//                            "defineClass",
//                            new Class[] { String.class, byte[].class, int.class, int.class });
//
//            // Protected method invocation.
//            method.setAccessible(true);
//            try {
//                Object[] args =
//                        new Object[] { className, b, new Integer(0), new Integer(b.length)};
//                clazz = (Class) method.invoke(loader, args);
//            } finally {
//                method.setAccessible(false);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//        return clazz;
//    }

}
