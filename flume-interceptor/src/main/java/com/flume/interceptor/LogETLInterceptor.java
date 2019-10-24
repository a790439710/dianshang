package com.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义拦截器，实现Interceptor接口，并且实现其抽象方法
 */
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        if(log.contains("start")){
            if(LogUtil.validateStart(log)){
                return event;
            }
        }else{
            if(LogUtil.validateEvent(log)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event: list){
            Event inteceptor = intercept(event);
            if(inteceptor != null) {
                interceptors.add(event);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Interceptor.Builder接口，并实现其抽象方法
     */
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
