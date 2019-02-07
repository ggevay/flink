package org.apache.flink.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.SimpleRemapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SpecUtil {

    private final static boolean enabled = true;

    private static final Map<String, Integer> copyCounts = new TreeMap<>();

    private static final Map<Tuple2<String, Object>, Class> cache = new HashMap<>();


    public static <T> T copyClassAndInstantiateNoCache(Class clazz, Object... ctorArgs) {
        return copyClassAndInstantiateNoCache(clazz.getName(), ctorArgs);
    }

    public static <T> T copyClassAndInstantiateNoCache(String name, Object... ctorArgs) {
        try {
            return (T) instantiate(copyClass(name), ctorArgs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T copyClassAndInstantiate(Object cacheKey, Class clazz, Object... ctorArgs) {
        return copyClassAndInstantiate(cacheKey, clazz.getName(), ctorArgs);
    }

    public static <T> T copyClassAndInstantiate(Object cacheKey, String name, Object... ctorArgs) {
        try {

            Tuple2<String, Object> internalKey = Tuple2.of(name, cacheKey);

            Class clazzToInst;
            synchronized (SpecUtil.class) {
                clazzToInst = cache.computeIfAbsent(internalKey, k -> copyClass(k.f0));
            }

            return (T) instantiate(clazzToInst, ctorArgs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Class<T> copyClass(String name) {
        try {
            if (!enabled) {
                return (Class) Class.forName(name);
            }

            ClassReader reader = new ClassReader(name);
            ClassWriter writer = new ClassWriter(reader, 0);

            int oldCount;
            synchronized (SpecUtil.class) {
                oldCount = copyCounts.getOrDefault(name, 0);
                copyCounts.put(name, oldCount + 1);
            }
            String newName = name + "__copy_" + oldCount;

            String nameWithSlashes = name.replace('.', '/');
            String newNameWithSlashes = newName.replace('.', '/');
            ClassVisitor visitor = new ClassRemapper(writer, new SimpleRemapper(nameWithSlashes, newNameWithSlashes));

            reader.accept(visitor, 0);

            return loadClass(newName, writer.toByteArray());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // automatically finds the appropriate ctor based on the given arguments' types
    private static Object instantiate(Class clazz, Object... args) throws IllegalAccessException, InvocationTargetException, InstantiationException {

        // We are looking for a ctor where all our args are assignable to the corresponding param (subclass or same type)
        Constructor mCtor = null;
        for (Constructor<?> ctor: clazz.getConstructors()) {
            Class<?>[] params = ctor.getParameterTypes();
            if (params.length == args.length) {
                boolean allMatch = true;
                int i = 0;
                for (Class<?> param : ctor.getParameterTypes()) {
                    Object arg = args[i++];
                    if (!(arg == null || param.isAssignableFrom(arg.getClass()))) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    mCtor = ctor;
                    break; // todo: maybe instead check whether there is only one overload that matches?
                }
            }
        }

        if (mCtor == null) {
            throw new RuntimeException("Ctor not found for " + clazz);
        }

        // Old code (doesn't work for arg types being subclasses of param types)
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

        return mCtor.newInstance(args);
    }


    // copy-paste from https://asm.ow2.io/faq.html#Q5
    private static Class loadClass(String className, byte[] b) {
        // Override defineClass (as it is protected) and define the class.
        Class clazz = null;
        try {
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            Class cls = Class.forName("java.lang.ClassLoader");
            java.lang.reflect.Method method =
                    cls.getDeclaredMethod(
                            "defineClass",
                            new Class[] { String.class, byte[].class, int.class, int.class });

            // Protected method invocation.
            method.setAccessible(true);
            try {
                Object[] args =
                        new Object[] { className, b, new Integer(0), new Integer(b.length)};
                clazz = (Class) method.invoke(loader, args);
            } finally {
                method.setAccessible(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return clazz;
    }
}
