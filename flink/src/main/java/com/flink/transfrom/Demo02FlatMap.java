package com.flink.transfrom;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo02FlatMap {
    public static void main(String[] args) throws Exception {
        /*
        * FlatMap 算子:对源数据进行转换处理,一条数据可以转换位多条数据
        * */

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据源
        DataStreamSource<String> ScoDS = env.socketTextStream("master", 8888);

        // 将传入进来的学生数据以类的方式存储
        ScoDS.flatMap((line, collector) -> {
            String[] LineDS = line.split(",");
            collector.collect(new FlatMapClass(LineDS[0],LineDS[1],Integer.parseInt(LineDS[2]),LineDS[3],LineDS[4]));
        }, Types.GENERIC(FlatMapClass.class)).print();

        // 启动任务
        env.execute();

    }
}
class FlatMapClass{
    String stuID;
    String Name;
    int age;
    String gender;
    String clazz;

    // 建立构造方法
    public FlatMapClass(String stuID, String name, int age, String gender, String clazz) {
        this.stuID = stuID;
        this.Name = name;
        this.age = age;
        this.gender = gender;
        this.clazz = clazz;
    }

    public String getStuID() {
        return stuID;
    }

    public void setStuID(String stuID) {
        this.stuID = stuID;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    // 建立toString()方法
    @Override
    public String toString() {
        return "FlatMapClass{" +
                "stuID='" + stuID + '\'' +
                ", Name='" + Name + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", clazz='" + clazz + '\'' +
                '}';
    }
}
