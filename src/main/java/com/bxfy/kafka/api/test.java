package com.bxfy.kafka.api;

import com.alibaba.fastjson.JSON;
import com.bxfy.kafka.api.entity.User;
import org.assertj.core.util.Lists;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @Author Mengdexin
 * @date 2022 -05 -09 -21:31
 */
public class test {

    public static void main(String[] args) throws ClassNotFoundException {
        //1.
        System.out.println(String.format("类的名称：%s", String.class.getName()));
        System.out.println(String.format("类的反射：%s", String.class));
        //2.
        User user = new User();
        System.out.println(String.format("类的反射：%s",user.getClass()));
        System.out.println(String.format("类的名称：%s",user.getClass().getName()));
        //3.
        System.out.println(String.format("类的反射：%s", Class.forName(User.class.getName())));
        Date date = new Date();
        System.out.println(String.format("时间数据类型: %s", date.getTime()));
        user.setDate(new Date(date.getTime()));
        System.out.println(user);
        System.out.println(JSON.toJSONString(user));
        System.out.println(JSON.toJSON(user));

        String json = "{'date':1652237200677}";

        User user1 = JSON.parseObject(json, User.class);
        System.out.println(user1);
    }

}
