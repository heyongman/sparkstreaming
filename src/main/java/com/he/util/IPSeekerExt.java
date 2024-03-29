package com.he.util;

import com.he.common.GlobalConstants;

import java.io.IOException;
import java.util.Properties;

public class IPSeekerExt extends IPSeeker {

	// 地址 仅仅只是在ecplise环境中使用，部署在服务器上，需要先将qqwry.dat放在集群的各个节点某个有读取权限目录，
	// 然后在这里指定全路径
	// private static final String ipFilePath = "ip/qqwry.dat";
	// 部署在服务器上
	// private static final String ipFilePath = "/opt/cdh-5.7.6/spark-2.2.0-bin-2.6.0-cdh5.7.6/etl-hfile-jars/qqwry.dat";

    // TODO: 通过配置属性文件，获取路径的值
    private static String ipFilePath = null ;
    private static Properties props = new Properties() ;
    // TODO: 静态方法块，加载属性配置文件
    static{
        // 加载配置文件
        try {
            props.load(IPSeekerExt.class.getClassLoader().getResourceAsStream("etl.properties"));

            // 本地模式运行
            if(Boolean.valueOf(props.getProperty("is.local"))){
                ipFilePath = props.getProperty("ip.datas.local.path") ;
            }else{
                ipFilePath = props.getProperty("ip.datas.path") ;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static IPSeekerExt obj = new IPSeekerExt(ipFilePath);
    
	protected IPSeekerExt(String ipFilePath) {
        super(ipFilePath);
    }

    public static IPSeekerExt getInstance() {
        return obj;
    }

    /**
     * 解析ip地址
     * 
     * @param ip
     * @return
     */
    public RegionInfo analyseIp(String ip) {
        if (ip == null || "".equals(ip.trim())) {
            return null;
        }

        RegionInfo info = new RegionInfo();
        try {
            String country = super.getCountry(ip);
            if ("局域网".equals(country) || country == null || country.isEmpty() || country.trim().startsWith("CZ88")) {
                // 设置默认值
                info.setCountry("中国");
                info.setProvince("上海市");
            } else {
                int length = country.length();
                int index = country.indexOf('省');
                if (index > 0) { // 表示是国内的某个省
                    info.setCountry("中国");
                    info.setProvince(country.substring(0, Math.min(index + 1, length)));
                    int index2 = country.indexOf('市', index);
                    if (index2 > 0) {
                        // 设置市
                        info.setCity(country.substring(index + 1, Math.min(index2 + 1, length)));
                    }
                } else {
                    String flag = country.substring(0, 2);
                    switch (flag) {
                    case "内蒙":
                        info.setCountry("中国");
                        info.setProvince("内蒙古自治区");
                        country = country.substring(3);
                        if (country != null && !country.isEmpty()) {
                            index = country.indexOf('市');
                            if (index > 0) {
                                // 设置市
                                info.setCity(country.substring(0, Math.min(index + 1, length)));
                            }
                            // TODO:针对其他旗或者盟没有进行处理
                        }
                        break;
                    case "广西":
                    case "西藏":
                    case "宁夏":
                    case "新疆":
                        info.setCountry("中国");
                        info.setProvince(flag);
                        country = country.substring(2);
                        if (country != null && !country.isEmpty()) {
                            index = country.indexOf('市');
                            if (index > 0) {
                                // 设置市
                                info.setCity(country.substring(0, Math.min(index + 1, length)));
                            }
                        }
                        break;
                    case "上海":
                    case "北京":
                    case "重庆":
                    case "天津":
                        info.setCountry("中国");
                        info.setProvince(flag + "市");
                        country = country.substring(3);
                        if (country != null && !country.isEmpty()) {
                            index = country.indexOf('区');
                            if (index > 0) {
                                // 设置市
                                char ch = country.charAt(index - 1);
                                if (ch != '小' || ch != '校') {
                                    info.setCity(country.substring(0, Math.min(index + 1, length)));
                                }
                            }

                            if (GlobalConstants.DEFAULT_VALUE.equals(info.getCity())) {
                                // 现在city还没有设置，考虑县
                                index = country.indexOf('县');
                                if (index > 0) {
                                    // 设置市
                                    info.setCity(country.substring(0, Math.min(index + 1, length)));
                                }
                            }
                        }
                        break;
                    case "香港":
                    case "澳门":
                        info.setCountry("中国");
                        info.setProvince(flag + "特别行政区");
                        break;
                    default:
                        info.setCountry(country); // 针对其他国外的ip
                    }
                }
            }
        } catch (Exception e) {
            // nothing
        }
        return info;
    }

    /**
     * ip地址对应的info类
     * 
     * @author ibeifeng
     *
     */
    public static class RegionInfo {
        private String country = GlobalConstants.DEFAULT_VALUE;
        private String province = GlobalConstants.DEFAULT_VALUE;
        private String city = GlobalConstants.DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo [country=" + country + ", province=" + province + ", city=" + city + "]";
        }
    }
}
