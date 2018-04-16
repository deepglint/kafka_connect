package com.justone.parser.json;

import com.deepglint.parser.json.Element;
import com.deepglint.parser.json.Parser;
import com.deepglint.parser.json.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Parser Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>一月 18, 2018</pre>
 */
public class ParserTest {
    private static Parser parser = new Parser();
    private String content1 = "{\"Uts\":\"2018-01-15T06:38:05Z\",\"Id\":\"e1cf2364-75f8-41c0-af0c-10d88eff0e74\",\"Ts\":1515997759350,\"SensorId\":\"";
    private String content2 = "\",\"FaceId\":\"";
    private String content3 = "\",\"ImageId\":\"143e5d15-8654-40c1-a00b-c83ceb44d8c2\",\"FaceReid\":\"";
    private String content4 = "\",\"Confidence\":0.98318744,\"GenderId\":2,\"GenderConfidence\":0.99852115,\"AgeId\":\"2\",\"AgeConfidence\":1,\"GlassId\":1,\"GlassConfidence\":0.999468,\"HatId\":1,\"HatConfidence\":0.99953294,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/87562\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"5b18f705-0172-4b5b-a84b-8cb49d8f0090\",\"GroupId\":\"177e6a8d-9f61-45dd-aad4-45164e342df2\",\"IsNew\":1,\"AlignScore\":0.98318744,\"Blur\":0.24167258,\"Pitch\":5.5959415,\"Roll\":0,\"Yaw\":1.6208384,\"ImageType\":2}";
    String columnList = "uts,ts,sensor_id,face_id,face_reid,confidence,gender_id,gender_confidence,age_id,age_confidence,glass_id,glass_confidence,cutboard_image_uri,cutboard_res_width,cutboard_res_height,status,image_type,pid,gid,is_new,wide_image_id,hat_id,hat_confidence,align_score,blur,pitch,roll,yaw";
    String pathList = "/@Uts,/@Ts,/@SensorId,/@Id,/@Id,/@Confidence,/@GenderId,/@GenderConfidence,/@AgeId,/@AgeConfidence,/@GlassId,/@GlassConfidence,/@CutboardImageUri,/@CutboardResWidth,/@CutboardResHeight,/@Status,/@ImageType,/@PersonId,/@GroupId,/@IsNew,/@ImageId,/@HatId,/@HatConfidence,/@AlignScore,/@Blur,/@Pitch,/@Roll,/@Yaw";
    Path[] iPaths = null;
    String [] columns;
    String [] paths;
    @Before
    public void before() throws Exception {
        columns=columnList.split("\\,");//split column list into separate strings
        paths=pathList.split("\\,");//split path list into separate strings
        iPaths = new Path[paths.length];//construct array of paths
        for (int i=0;i<paths.length;++i) {//for each path
            iPaths[i]=new Path(paths[i]);//construct path from path string
        }
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: parse(String aMessage)
     */
    @Test
    public void testParse() throws Exception {
        //TODO: Test goes here...
        /*String tmp = "thomugo";
        String message = content1 + "filter-test"+tmp + content2 + tmp + content3 + tmp + content4;
        try{
            parser.parse(message);
        }catch (RuntimeException e){
            e.printStackTrace();
        }*/
        String message5 = "{\"Uts\":\"2018-01-15T06:38:21Z\",\"Id\":\"b03b3a79-abce-4e80-8d38-ab1d43f860d9\",\"Ts\":1515998162614,\"SensorId\":\"8096d671-1003-4896-b94d-23174fa66e62\",\"FaceId\":\"\",\"ImageId\":\"1982a124-7190-40ca-9443-cb750b607777\",\"FaceReid\":\"\",\"Confidence\":0.36766842,\"GenderId\":1,\"GenderConfidence\":0.9432012,\"AgeId\":\"false\",\"AgeConfidence\":1,\"GlassId\":1,\"GlassConfidence\":0.9999008,\"HatId\":1,\"HatConfidence\":0.6396236,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/127478\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"f6478034-2b2a-4159-8955-74c87616b029\",\"GroupId\":\"1a80d8d8-6b42-486e-8e2d-e9873dc6ea59\",\"IsNew\":1,\"AlignScore\":0.36766842,\"Blur\":0.32964122,\"Pitch\":-17.172184,\"Roll\":0,\"Yaw\":0.50636417,\"ImageType\":2}";
        String message4 = "{\"Uts\":\"2018-01-15T06:38:05Z\",\"Id\":\"fc19d654-3dbe-4ff5-981a-3eb9687db87a\",\"Ts\":1515997740406,\"SensorId\":\"8abfc0d0-30a3-4c71-be6b-beb5f4b347d1\",\"FaceId\":\"\",\"ImageId\":\"0d983c92-15bc-4ea2-a6a3-23dc0fe76031\",\"FaceReid\":\"\",\"Confidence\":0.9820661,\"GenderId\":1,\"GenderConfidence\":0.9933289,\"AgeId\":\"32\",\"AgeConfidence\":1,\"GlassId\":1,\"GlassConfidence\":0.99917454,\"HatId\":1,\"HatConfidence\":0.9983935,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/85699\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"3a1e8e98-b7dc-432d-9680-f25533b90779\",\"GroupId\":\"4f0af7ae-1199-4aa7-a358-402844a72989\",\"IsNew\":1,\"AlignScore\":0.9820661,\"Blur\":0.13070053,\"Pitch\":4.6160827,\"Roll\":0,\"Yaw\":-14.041861,\"ImageType\":2}";
        String message3 = "{\"Uts\":\"2018-01-15T06:38:21Z\",\"Id\":\"b667b096-0969-45c1-ba01-6fe10afe2d6b\",\"Ts\":1515998033128,\"SensorId\":\"568d831c-2c76-48b3-99d8-837bcb4fb6ab\",\"FaceId\":\"\",\"ImageId\":\"fd531ef6-57c7-43a8-8963-79fc7d33c920\",\"FaceReid\":\"\",\"Confidence\":0.99759996,\"GenderId\":1,\"GenderConfidence\":0.9984189,\"AgeId\":\"28\",\"AgeConfidence\":1,\"GlassId\":1,\"GlassConfidence\":0.99895763,\"HatId\":1,\"HatConfidence\":0.9903487,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/114658\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"d0971c32-13ee-442e-9f70-b627e823bb85\",\"GroupId\":\"854a2317-b385-4058-aa21-3222e4fedf4b\",\"IsNew\":1,\"AlignScore\":0.99759996,\"Blur\":0.85015947,\"Pitch\":15.648177,\"Roll\":0,\"Yaw\":8.795221,\"ImageType\":2}";
        String message1 = "{\"Uts\":\"2018-01-15T06:38:05Z\",\"Id\":\"fc19d654-3dbe-4ff5-981a-3eb9687db87a\",\"Ts\":1515997740406,\"SensorId\":\"8abfc0d0-30a3-4c71-be6b-beb5f4b347d1\",\"FaceId\":\"\",\"ImageId\":\"0d983c92-15bc-4ea2-a6a3-23dc0fe76031\",\"FaceReid\":\"\",\"Confidence\":0.9820661,\"GenderId\":1,\"GenderConfidence\":0.9933289,\"AgeId\":\"32\",\"AgeConfidence\":1,\"GlassId\":1,\"GlassConfidence\":0.99917454,\"HatId\":1,\"HatConfidence\":0.9983935,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/85699\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"3a1e8e98-b7dc-432d-9680-f25533b90779\",\"GroupId\":\"4f0af7ae-1199-4aa7-a358-402844a72989\",\"IsNew\":1,\"AlignScore\":0.9820661,\"Blur\":0.13070053,\"Pitch\":4.6160827,\"Roll\":0,\"Yaw\":-14.041861,\"ImageType\":2}";
        String message2 = "{\"Uts\":\"2018-01-15T06:38:05Z\",\"Id\":\"cff1257c-7fc8-46fb-b55f-53380c494837\",\"Ts\":1515997966141,\"SensorId\":\"ae75aa7e-13d0-434e-a4c5-4d4cd9e22c6c\",\"FaceId\":\"\",\"ImageId\":\"c9d6646e-604d-4bc9-8741-f473872cd624\",\"FaceReid\":\"\",\"Confidence\":0.9647936,\"GenderId\":1,\"GenderConfidence\":0.9962347,\"AgeId\":\"23\",\"AgeConfidence\":1,\"GlassId\":2,\"GlassConfidence\":0.9927407,\"HatId\":1,\"HatConfidence\":0.9872481,\"CutboardImageUri\":\"http://dgtest.ufile.ucloud.com.cn/108035\",\"CutboardResWidth\":500,\"CutboardResHeight\":330,\"Status\":0,\"PersonId\":\"aa54086e-d094-49a2-87b9-b9a19901194d\",\"GroupId\":\"d4db8219-2c8c-4a08-8924-749e49264f53\",\"IsNew\":1,\"AlignScore\":0.9647936,\"Blur\":0.42960113,\"Pitch\":12.1464615,\"Roll\":0,\"Yaw\":-5.3264575,\"ImageType\":2}";
        parser.parse(message4);
        for (int i=0;i<iPaths.length;++i) {//for each parse path
            Element element = parser.getElement(iPaths[i]);
            System.out.println(columns[i] + " : " +element.toString());
            /*if (element==null) { //if no element found
                //append nothing
            } else {//else element found
                String string=element.toString();//convert element to string
                if (string.equals("null")) {//if "null" string
                    //append nothing
                } else {//else other than "null" string
                    if (string.charAt(0)=='"') {//if enclosed in quotes
                        System.out.println("append: " + string.substring(1, string.length() - 1));
                    } else {//else not enclosed i quotes
                        System.out.println("append: " + string);
                    }
                }//if "null" string
            }*/
        }
    }

    /**
     * Method: getRootElement()
     */
    @Test
    public void testGetRootElement() throws Exception {
        //TODO: Test goes here...
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: contains(Path aPath)
     */
    @Test
    public void testContains() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: containsAll(Path[] aPaths)
     */
    @Test
    public void testContainsAll() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: containsAny(Path[] aPaths)
     */
    @Test
    public void testContainsAny() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getElement(Path aPath)
     */
    @Test
    public void testGetElement() throws Exception {
//TODO: Test goes here... 
    }


    /**
     * Method: context()
     */
    @Test
    public void testContext() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("context"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: next()
     */
    @Test
    public void testNext() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("next"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: peek()
     */
    @Test
    public void testPeek() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("peek"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: back()
     */
    @Test
    public void testBack() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("back"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: skip(int aSkip)
     */
    @Test
    public void testSkip() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("skip", int.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseString()
     */
    @Test
    public void testParseString() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseString"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseNumber()
     */
    @Test
    public void testParseNumber() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseNumber"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseBoolean()
     */
    @Test
    public void testParseBoolean() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseBoolean"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseNull()
     */
    @Test
    public void testParseNull() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseNull"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseRoot()
     */
    @Test
    public void testParseRoot() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseRoot"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseArray()
     */
    @Test
    public void testParseArray() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseArray"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: parseObject()
     */
    @Test
    public void testParseObject() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = Parser.getClass().getMethod("parseObject"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

} 
