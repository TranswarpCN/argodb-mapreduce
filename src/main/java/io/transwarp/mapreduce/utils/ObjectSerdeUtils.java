package io.transwarp.mapreduce.utils;

import java.io.*;
import java.util.Base64;

public class ObjectSerdeUtils {

  public static String serialize(Object obj) throws IOException {
    if (obj == null) {
      return "";
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(obj);
    byteArrayOutputStream.close();
    return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
  }

  public static Object deserialize(String str) throws IOException, ClassNotFoundException {
    if (str == null || str.isEmpty()) {
      return null;
    }
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(Base64.getDecoder().decode(str));
    ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
    return objectInputStream.readObject();
  }

}
