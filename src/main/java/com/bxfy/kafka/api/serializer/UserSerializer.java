package com.bxfy.kafka.api.serializer;

import com.bxfy.kafka.api.entity.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.common.UnknownCodecException;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author Mengdexin
 * @date 2022 -05 -10 -15:16
 */
public class UserSerializer implements Serializer<User>, Deserializer<User> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * 进行序列化
     */
    @SneakyThrows
    @Override
    public byte[] serialize(String topic, User user) {
        if (null == user) {
            return new byte[0];
        }
        try {
            byte[] idBytes = new byte[0];
            byte[] nameBytes = new byte[0];
            if (null != user.getId()) {
                idBytes = user.getId().getBytes("UTF-8");
            }
            if (null != user.getName()) {
                nameBytes = user.getName().getBytes("UTF-8");
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + idBytes.length + nameBytes.length);
            byteBuffer.putInt(idBytes.length);
            byteBuffer.put(idBytes);
            byteBuffer.putInt(nameBytes.length);
            byteBuffer.put(nameBytes);
            return byteBuffer.array();
        }catch (UnknownCodecException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (null == data) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("反序列化失败");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        //id字节数组的长度
        int idInt = buffer.getInt();
        byte[] idBytes = new byte[idInt];
        buffer.get(idBytes);
        //name的
        int nameInt = buffer.getInt();
        byte[] nameBytes = new byte[nameInt];
        buffer.get(nameBytes);

        //将字节数组转成对象
        String id, name;
        try {
        id = new String(idBytes, "UTF-8");
        name = new String(nameBytes, "UTF-8");
        } catch (Exception e) {
            throw new SerializationException("反序列化失败");
        }
        return new User(id, name);
    }

}
