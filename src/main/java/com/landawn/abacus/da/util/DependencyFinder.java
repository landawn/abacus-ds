/*
 * Copyright (c) 2015, Holger and aro_tech. Refer to: http://stackoverflow.com/questions/19862866/find-java-class-dependencies-at-runtime
 */
package com.landawn.abacus.da.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.function.Predicate;

// TODO: Auto-generated Javadoc
/**
 * The Class DependencyFinder.
 */
public final class DependencyFinder {
    //    public static void main(String[] args) {
    //        try {
    //            // Get dependencies for my class:
    //            Set<Class<?>> dependencies = getDependencies(Class.forName("com.example.MyClass")); // REPLACE WITH YOUR CLASS NAME
    //
    //            Set<Class<?>> a = N.filter(dependencies, new Predicate<Class<?>>() {
    //                @Override
    //                public boolean test(Class<?> cls) {
    //                    final String canonicalClassName = N.getCanonicalClassName(cls);
    //                    return !(canonicalClassName.startsWith("java.") || canonicalClassName.startsWith("com.landawn.abacus"));
    //                }
    //            });
    //
    //            N.println(a);
    //        } catch (Exception e) {
    //            e.printStackTrace();
    //        }
    //    }

    /**
     * Get the set of direct dependencies for the given class.
     *
     * @param classToCheck
     * @return
     */
    public static Set<Class<?>> getDependencies(final Class<?> classToCheck) {
        return getDependencies(classToCheck, null);
    }

    /**
     * Get the set of direct dependencies for the given class.
     *
     * @param classToCheck
     * @param classNamefilter
     * @return The direct dependencies for classToCheck, as a set of classes
     */
    public static Set<Class<?>> getDependencies(final Class<?> classToCheck, final Predicate<? super String> classNamefilter) {
        final Class<?> adjustedClassToCheck = adjustSourceClassIfArray(classToCheck);

        if (adjustedClassToCheck.isPrimitive()) {
            return Collections.emptySet();
        }

        final String clsName = getClassName(classToCheck);

        if (classNamefilter.test(clsName)) {
            return N.emptySet();
        }

        try {
            return mapClassNamesToClasses(adjustedClassToCheck, getDependenciesFromClassBytes(readClassBytes(adjustedClassToCheck)), classNamefilter);
        } catch (ClassNotFoundException e) {
            throw N.toRuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the class name.
     *
     * @param cls
     * @return
     */
    private static String getClassName(final Class<?> cls) {
        return cls.getName();
    }

    /**
     * Get the set of dependencies for the given class recursively.
     *
     * @param classToCheck
     * @return
     */
    public static Set<Class<?>> getDependenciesRecursively(final Class<?> classToCheck) {
        return getDependenciesRecursively(classToCheck, null);
    }

    /**
     * Get the set of dependencies for the given class recursively.
     *
     * @param classToCheck
     * @param classNamefilter
     * @return The direct dependencies for classToCheck, as a set of classes
     */
    public static Set<Class<?>> getDependenciesRecursively(final Class<?> classToCheck, final Predicate<? super String> classNamefilter) {
        final Set<Class<?>> foundClasses = N.newHashSet();
        final Set<Class<?>> resultClasses = getDependenciesRecursively(classToCheck, classNamefilter, foundClasses);
        resultClasses.remove(classToCheck);
        return resultClasses;
    }

    /**
     * Gets the dependencies recursively.
     *
     * @param classToCheck
     * @param classNamefilter
     * @param foundClasses
     * @return
     */
    private static Set<Class<?>> getDependenciesRecursively(final Class<?> classToCheck, final Predicate<? super String> classNamefilter,
            final Set<Class<?>> foundClasses) {
        if (foundClasses.contains(classToCheck)) {
            return N.emptySet();
        }

        foundClasses.add(classToCheck);

        final Set<Class<?>> resultClasses = N.newLinkedHashSet();
        final Set<Class<?>> classes = getDependencies(classToCheck, classNamefilter);
        resultClasses.addAll(classes);

        for (Class<?> cls : classes) {
            resultClasses.addAll(getDependenciesRecursively(cls, classNamefilter, foundClasses));
        }

        return resultClasses;
    }

    /**
     * Adjust source class if array.
     *
     * @param sourceClass
     * @return
     */
    private static Class<?> adjustSourceClassIfArray(final Class<?> sourceClass) {
        Class<?> adjustedSourceClass = sourceClass;
        while (adjustedSourceClass.isArray()) {
            adjustedSourceClass = sourceClass.getComponentType();
        }
        return adjustedSourceClass;
    }

    /**
     * Map class names to classes.
     *
     * @param from
     * @param names
     * @param classNamefilter
     * @return
     * @throws ClassNotFoundException the class not found exception
     */
    private static Set<Class<?>> mapClassNamesToClasses(Class<?> from, Set<String> names, final Predicate<? super String> classNamefilter)
            throws ClassNotFoundException {
        final ClassLoader cl = from.getClassLoader();
        final Set<Class<?>> classes = N.newLinkedHashSet(names.size());

        for (String name : names) {
            if (classNamefilter.test(name) == false) {
                classes.add(Class.forName(name, false, cl));
            }
        }

        classes.remove(from);// remove self-reference
        return classes;
    }

    /**
     * Read class bytes.
     *
     * @param from
     * @return
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static ByteBuffer readClassBytes(Class<?> from) throws IOException {
        Buffer readBuf = new Buffer();
        InputStream is = null;
        // N.println(from.getName());
        try {
            String clsName = getClassName(from);
            int lastIndex = clsName.lastIndexOf('.');
            is = from.getResourceAsStream((lastIndex < 0 ? clsName : clsName.substring(lastIndex + 1)) + ".class");

            int byteCountFromLastRead = 0;
            do {
                readBuf.read += byteCountFromLastRead;
                adustBufferSize(readBuf, is);
                byteCountFromLastRead = is.read(readBuf.buf, readBuf.read, readBuf.buf.length - readBuf.read);
            } while (byteCountFromLastRead > 0);
        } finally {
            IOUtil.closeQuietly(is);
        }
        return readBuf.toByteBuffer();
    }

    /**
     * Adust buffer size.
     *
     * @param readBuf
     * @param is
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static void adustBufferSize(Buffer readBuf, InputStream is) throws IOException {
        int bufferSize = Math.max(is.available() + 100, 100);
        if (readBuf.buf == null) {
            readBuf.buf = new byte[bufferSize];
        } else if (readBuf.buf.length - readBuf.read < bufferSize) {
            System.arraycopy(readBuf.buf, 0, readBuf.buf = new byte[readBuf.read + bufferSize], 0, readBuf.read);
        }
    }

    /**
     * Gets the dependencies from class bytes.
     *
     * @param readBuffer
     * @return
     */
    private static Set<String> getDependenciesFromClassBytes(ByteBuffer readBuffer) {
        verifyMagicFileTypeHeader(readBuffer);
        final int constantPoolItemCount = getConstantPoolItemCount(readBuffer);
        ConstantPoolItemFlags flags = new ConstantPoolItemFlags(constantPoolItemCount);
        flagConstantPoolItemsAsDependencies(readBuffer, constantPoolItemCount, flags);
        return extractClassNamesFromConstantsBasedOnFlags(readBuffer, constantPoolItemCount, flags);
    }

    /**
     * Flag constant pool items as dependencies.
     *
     * @param readBuffer
     * @param constantPoolItemCount
     * @param flags
     */
    private static void flagConstantPoolItemsAsDependencies(ByteBuffer readBuffer, final int constantPoolItemCount, ConstantPoolItemFlags flags) {
        for (int c = 1; c < constantPoolItemCount; c++) {
            c = readOneConstantPoolItemAndSetFlagIfClassOrNamedType(readBuffer, flags, c);
        }
        skipPastAccessFlagsThisClassAndSuperClass(readBuffer);
        skipInterfaces(readBuffer);
        flagFieldsAndMethodsAsNamedTypes(readBuffer, flags.isNamedType);
    }

    /**
     * Gets the constant pool item count.
     *
     * @param readBuffer
     * @return
     */
    private static int getConstantPoolItemCount(ByteBuffer readBuffer) {
        setCursorToConstantPoolCountPosition(readBuffer);
        final int constantPoolCount = readBuffer.getChar();
        return constantPoolCount;
    }

    /**
     *
     * @param readBuffer
     */
    private static void skipInterfaces(ByteBuffer readBuffer) {
        readBuffer.position(readBuffer.getChar() * 2 + readBuffer.position());
    }

    /**
     * Skip past access flags this class and super class.
     *
     * @param readBuffer
     */
    private static void skipPastAccessFlagsThisClassAndSuperClass(ByteBuffer readBuffer) {
        skipBytes(readBuffer, 6);
    }

    /**
     * Extract class names from constants based on flags.
     *
     * @param readBuffer
     * @param numberOfConstants
     * @param flags
     * @return
     * @throws AssertionError the assertion error
     */
    private static Set<String> extractClassNamesFromConstantsBasedOnFlags(ByteBuffer readBuffer, final int numberOfConstants, ConstantPoolItemFlags flags)
            throws AssertionError {
        Set<String> names = N.newLinkedHashSet();
        returnBufferToStartOfConstantPool(readBuffer);
        for (int constantPoolIndex = 1; constantPoolIndex < numberOfConstants; constantPoolIndex++) {
            switch (readBuffer.get()) {
                case CONSTANT_Utf8:
                    readClassNamesInUTF8Value(readBuffer, flags, names, constantPoolIndex);
                    break;
                case CONSTANT_Integer:
                case CONSTANT_Float:
                case CONSTANT_FieldRef:
                case CONSTANT_MethodRef:
                case CONSTANT_InterfaceMethodRef:
                case CONSTANT_NameAndType:
                case CONSTANT_InvokeDynamic:
                    skipBytes(readBuffer, 4);
                    break;
                case CONSTANT_Long:
                case CONSTANT_Double:
                    skipBytes(readBuffer, 8);
                    constantPoolIndex++; // long or double counts as 2 items
                    break;
                case CONSTANT_String:
                case CONSTANT_Class:
                case CONSTANT_MethodType:
                    skipBytes(readBuffer, 2);
                    break;
                case CONSTANT_MethodHandle:
                    skipBytes(readBuffer, 3);
                    break;
                default:
                    throw new AssertionError();
            }
        }
        return names;
    }

    /**
     * Read class names in UTF 8 value.
     *
     * @param readBuffer
     * @param flags
     * @param dependencyClassNames
     * @param constantNumber
     */
    private static void readClassNamesInUTF8Value(ByteBuffer readBuffer, ConstantPoolItemFlags flags, Set<String> dependencyClassNames, int constantNumber) {
        int strSize = readBuffer.getChar(), strStart = readBuffer.position();
        boolean multipleNames = flags.isNamedType.get(constantNumber);
        if (flags.isClass.get(constantNumber)) {
            if (readBuffer.get(readBuffer.position()) == ARRAY_START_CHAR) {
                multipleNames = true;
            } else {
                addClassNameToDependencySet(dependencyClassNames, readBuffer, strStart, strSize);
            }
        }
        if (multipleNames) {
            addClassNamesToDependencySet(dependencyClassNames, readBuffer, strStart, strSize);
        }
        readBuffer.position(strStart + strSize);
    }

    /**
     * Flag fields and methods as named types.
     *
     * @param readBuffer
     * @param isNamedType
     */
    private static void flagFieldsAndMethodsAsNamedTypes(ByteBuffer readBuffer, BitSet isNamedType) {
        for (int type = 0; type < 2; type++) { // fields and methods
            int numMember = readBuffer.getChar();
            for (int member = 0; member < numMember; member++) {
                skipBytes(readBuffer, 4);
                isNamedType.set(readBuffer.getChar());
                int numAttr = readBuffer.getChar();
                for (int attr = 0; attr < numAttr; attr++) {
                    skipBytes(readBuffer, 2);
                    readBuffer.position(readBuffer.getInt() + readBuffer.position());
                }
            }
        }
    }

    /**
     * Return buffer to start of constant pool.
     *
     * @param readBuffer
     */
    private static void returnBufferToStartOfConstantPool(ByteBuffer readBuffer) {
        readBuffer.position(10);
    }

    /**
     * Read one constant pool item and set flag if class or named type.
     *
     * @param readBuffer
     * @param flags
     * @param currentConstantIndex
     * @return
     */
    private static int readOneConstantPoolItemAndSetFlagIfClassOrNamedType(ByteBuffer readBuffer, ConstantPoolItemFlags flags, int currentConstantIndex) {
        switch (readBuffer.get()) {
            case CONSTANT_Utf8:
                skipPastVariableLengthString(readBuffer);
                break;
            case CONSTANT_Integer:
            case CONSTANT_Float:
            case CONSTANT_FieldRef:
            case CONSTANT_MethodRef:
            case CONSTANT_InterfaceMethodRef:
            case CONSTANT_InvokeDynamic:
                skipBytes(readBuffer, 4);
                break;
            case CONSTANT_Long:
            case CONSTANT_Double:
                skipBytes(readBuffer, 8);
                currentConstantIndex++;
                break;
            case CONSTANT_String:
                skipBytes(readBuffer, 2);
                break;
            case CONSTANT_NameAndType:
                skipBytes(readBuffer, 2);// skip name, fall through to flag as a
                                         // named type:
            case CONSTANT_MethodType:
                flags.isNamedType.set(readBuffer.getChar()); // flag as named type
                break;
            case CONSTANT_Class:
                flags.isClass.set(readBuffer.getChar()); // flag as class
                break;
            case CONSTANT_MethodHandle:
                skipBytes(readBuffer, 3);
                break;
            default:
                throw new IllegalArgumentException("constant pool item type " + (readBuffer.get(readBuffer.position() - 1) & 0xff));
        }
        return currentConstantIndex;
    }

    /**
     *
     * @param readBuffer
     * @param bytesToSkip
     */
    private static void skipBytes(ByteBuffer readBuffer, int bytesToSkip) {
        readBuffer.position(readBuffer.position() + bytesToSkip);
    }

    /**
     * Skip past variable length string.
     *
     * @param readBuffer
     */
    private static void skipPastVariableLengthString(ByteBuffer readBuffer) {
        readBuffer.position(readBuffer.getChar() + readBuffer.position());
    }

    /**
     * Sets the cursor to constant pool count position.
     *
     * @param readBuffer the new cursor to constant pool count position
     */
    private static void setCursorToConstantPoolCountPosition(ByteBuffer readBuffer) {
        readBuffer.position(8);
    }

    /**
     * Verify magic file type header.
     *
     * @param readBuffer
     */
    private static void verifyMagicFileTypeHeader(ByteBuffer readBuffer) {
        if (readBuffer.getInt() != 0xcafebabe) {
            throw new IllegalArgumentException("Not a class file");
        }
    }

    /**
     * Adds the class name to dependency set.
     *
     * @param names
     * @param readBuffer
     * @param start
     * @param length
     */
    private static void addClassNameToDependencySet(Set<String> names, ByteBuffer readBuffer, int start, int length) {
        final int end = start + length;
        StringBuilder dst = new StringBuilder(length);
        ascii: {
            for (; start < end; start++) {
                byte b = readBuffer.get(start);
                if (b < 0) {
                    break ascii;
                }
                dst.append((char) (b == '/' ? '.' : b));
            }
            names.add(dst.toString());
            return;
        }
        final int oldLimit = readBuffer.limit(), oldPos = dst.length();
        readBuffer.limit(end).position(start);
        dst.append(StandardCharsets.UTF_8.decode(readBuffer));
        readBuffer.limit(oldLimit);
        for (int pos = oldPos, len = dst.length(); pos < len; pos++) {
            if (dst.charAt(pos) == '/') {
                dst.setCharAt(pos, '.');
            }
        }
        names.add(dst.toString());
        return;
    }

    /**
     * Adds the class names to dependency set.
     *
     * @param names
     * @param readBuffer
     * @param start
     * @param length
     */
    private static void addClassNamesToDependencySet(Set<String> names, ByteBuffer readBuffer, int start, int length) {
        final int end = start + length;
        for (; start < end; start++) {
            if (readBuffer.get(start) == 'L') {
                int endMarkerPosition = start + 1;
                while (readBuffer.get(endMarkerPosition) != ';') {
                    endMarkerPosition++;
                }
                addClassNameToDependencySet(names, readBuffer, start + 1, calculateLength(start, endMarkerPosition));
                start = endMarkerPosition;
            }
        }
    }

    /**
     *
     * @param start
     * @param endMarkerPosition
     * @return
     */
    private static int calculateLength(int start, int endMarkerPosition) {
        return endMarkerPosition - start - 1;
    }

    /** The Constant ARRAY_START_CHAR. */
    private static final char ARRAY_START_CHAR = '[';

    /** The Constant CONSTANT_InvokeDynamic. */
    // Constant pool data type constants:
    private static final byte CONSTANT_Utf8 = 1, CONSTANT_Integer = 3, CONSTANT_Float = 4, CONSTANT_Long = 5, CONSTANT_Double = 6, CONSTANT_Class = 7,
            CONSTANT_String = 8, CONSTANT_FieldRef = 9, CONSTANT_MethodRef = 10, CONSTANT_InterfaceMethodRef = 11, CONSTANT_NameAndType = 12,
            CONSTANT_MethodHandle = 15, CONSTANT_MethodType = 16, CONSTANT_InvokeDynamic = 18;

    /**
     * The Class Buffer.
     */
    // encapsulate byte buffer with its read count:
    private static class Buffer {

        /** The buf. */
        byte[] buf = null;

        /** The read. */
        int read = 0;

        /**
         * To byte buffer.
         *
         * @return
         */
        // convert to ByteBuffer
        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(this.buf, 0, this.read);
        }
    }

    /**
     * The Class ConstantPoolItemFlags.
     */
    // flags for identifying dependency names in the constant pool
    private static class ConstantPoolItemFlags {

        /** The is class. */
        final BitSet isClass;

        /** The is named type. */
        final BitSet isNamedType;

        /**
         * Instantiates a new constant pool item flags.
         *
         * @param constantPoolItemCount
         */
        ConstantPoolItemFlags(int constantPoolItemCount) {
            isClass = new BitSet(constantPoolItemCount);
            isNamedType = new BitSet(constantPoolItemCount);
        }
    }
}
