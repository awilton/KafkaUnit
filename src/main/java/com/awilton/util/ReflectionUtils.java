package com.awilton.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.junit.runners.model.TestClass;

public abstract class ReflectionUtils {
  
 
  public static void makeAccessible(Field field) {
    if ((!Modifier.isPublic(field.getModifiers()) || 
         !Modifier.isPublic(field.getDeclaringClass().getModifiers()) ||
         Modifier.isFinal(field.getModifiers())) &&!field.isAccessible())
    {
      field.setAccessible(true);
    }   
  }
  
  public static void inject(Field f, Object o) throws Throwable {
    makeAccessible(f);
    f.set(o, f);
  }
  
}
