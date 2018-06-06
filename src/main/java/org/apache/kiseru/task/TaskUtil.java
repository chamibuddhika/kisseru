package org.apache.kiseru.task;

import org.apache.kiseru.annotation.TaskOutPort;
import org.apache.kiseru.annotation.TaskParam;
import org.apache.kiseru.pipeline.Activity;
import org.apache.kiseru.pipeline.OutPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskUtil {

    private final static Logger logger = LoggerFactory.getLogger(TaskUtil.class);

//    public static <T extends Activity> List<OutPort> getOutPortsOfTask(T taskObj) throws IllegalAccessException {
//
//        List<OutPort> outPorts = new ArrayList<>();
//        for (Class<?> c = taskObj.getClass(); c != null; c = c.getSuperclass()) {
//            Field[] fields = c.getDeclaredFields();
//            for (Field field : fields) {
//                TaskOutPort outPortAnnotation = field.getAnnotation(TaskOutPort.class);
//                if (outPortAnnotation != null) {
//                    field.setAccessible(true);
//                    OutPort outPort = (OutPort) field.get(taskObj);
//                    outPorts.add(outPort);
//                }
//            }
//        }
//        return outPorts;
//    }
//
//    public static <T extends Activity> Map<String, String> serializeTaskData(T data) throws IllegalAccessException {
//
//        Map<String, String> result = new HashMap<>();
//        for (Class<?> c = data.getClass(); c != null; c = c.getSuperclass()) {
//            Field[] fields = c.getDeclaredFields();
//            for (Field classField : fields) {
//                TaskParam parm = classField.getAnnotation(TaskParam.class);
//                if (parm != null) {
//                    classField.setAccessible(true);
//                    result.put(parm.name(), classField.get(data).toString());
//                }
//
//                TaskOutPort outPort = classField.getAnnotation(TaskOutPort.class);
//                if (outPort != null) {
//                    classField.setAccessible(true);
//                    if (classField.get(data) != null) {
//                        result.put(outPort.name(), ((OutPort) classField.get(data)).getNextJobId().toString());
//                    }
//                }
//            }
//        }
//        return result;
//    }
//
//    public static <T extends Activity> void deserializeTaskData(T instance, Map<String, String> params) throws IllegalAccessException, InstantiationException {
//
//        List<Field> allFields = new ArrayList<>();
//        Class genericClass = instance.getClass();
//
//        while (Activity.class.isAssignableFrom(genericClass)) {
//            Field[] declaredFields = genericClass.getDeclaredFields();
//            for (Field declaredField : declaredFields) {
//                allFields.add(declaredField);
//            }
//            genericClass = genericClass.getSuperclass();
//        }
//
//        for (Field classField : allFields) {
//            TaskParam param = classField.getAnnotation(TaskParam.class);
//            if (param != null) {
//                if (params.containsKey(param.name())) {
//                    classField.setAccessible(true);
//                    if (classField.getType().isAssignableFrom(String.class)) {
//                        classField.set(instance, params.get(param.name()));
//                    } else if (classField.getType().isAssignableFrom(Integer.class) ||
//                            classField.getType().isAssignableFrom(Integer.TYPE)) {
//                        classField.set(instance, Integer.parseInt(params.get(param.name())));
//                    } else if (classField.getType().isAssignableFrom(Long.class) ||
//                            classField.getType().isAssignableFrom(Long.TYPE)) {
//                        classField.set(instance, Long.parseLong(params.get(param.name())));
//                    } else if (classField.getType().isAssignableFrom(Boolean.class) ||
//                            classField.getType().isAssignableFrom(Boolean.TYPE)) {
//                        classField.set(instance, Boolean.parseBoolean(params.get(param.name())));
//                    }
//                }
//            }
//        }
//
//        for (Field classField : allFields) {
//            TaskOutPort outPort = classField.getAnnotation(TaskOutPort.class);
//            if (outPort != null) {
//                classField.setAccessible(true);
//                if (params.containsKey(outPort.name())) {
//                    classField.set(instance, new OutPort(params.get(outPort.name()), instance));
//                } else {
//                    classField.set(instance, new OutPort(null, instance));
//                }
//            }
//        }
//    }
}
