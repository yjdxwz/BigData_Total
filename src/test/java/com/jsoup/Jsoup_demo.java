package com.jsoup;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class Jsoup_demo {
    @Test
    public void test() throws Exception{
    Document doc = Jsoup.connect("http://www.yiibai.com").get();
    String title = doc.title();
    System.out.println("title is: " + title);
}
    @Test
    public void test1() throws Exception{
        String html = "<div><p>Lorem ipsum.</p>";
        Document doc = Jsoup.parseBodyFragment(html);
        Element body = doc.body();
        body.addClass("aaa");
        System.out.println(body);
    }
    @Test
    public void test2() throws Exception{
        try
        {
            Document document = Jsoup.parse( new File("D:/temp/index.html" ) , "utf-8" );
            System.out.println(document.title());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
/***
 * 获取标题
 * */
    @Test
    public void test3() throws Exception{
    try
    {
        Document document = Jsoup.parse( new File("C:/Users/xyz/Desktop/yiibai-index.html"),
                "utf-8");
        System.out.println(document.title());
    }
    catch (IOException e)
    {
        e.printStackTrace();
    }}

    /**
     * 5. 获取HTML页面的Fav图标
     * */
    @Test
    public void test4() throws Exception{
        String favImage = "Not Found";
        try {
            //C:/Users/zkpkhua/Desktop/yiibai-index.html
            Document document = Jsoup.parse(new File("E:/eg/110003098.htm"), "utf-8");
            Element element = document.head().select("link[href~=.*\\.(ico|png)]").first();
            if (element == null)
            {
                element = document.head().select("meta[itemprop=image]").first();
                if (element != null)
                {
                    favImage = element.attr("content");
                }
            }
            else
            {
                favImage = element.attr("href");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println(favImage);
    }
    @Test
    public void test5() throws Exception{
        try
    {
        Document document = Jsoup.connect("https://www.zhipin.com/").get();
        System.out.println(document.title());
        Elements links = document.select("a[href]");
        for (Element link : links)
        {
            System.out.println("link : " + link.attr("href"));
            System.out.println("text : " + link.text());
        }
    }
        catch (IOException e)
    {
        e.printStackTrace();
    }}

    public String getWhiteList(String str){

        Whitelist white = new Whitelist();
        white.addTags(new String[] { "body" });
        white.addTags(new String[] { "p" });
        white.addTags(new String[] { "sub" });
        white.addTags(new String[] { "i" });
        white.addTags(new String[] { "sup" });
        white.addTags(new String[] { "h1" });
        white.addTags(new String[] { "h2" });
        white.addTags(new String[] { "h3" });
        white.addTags(new String[] { "h4" });
        white.addTags(new String[] { "h5" });
        white.addTags(new String[] { "h6" });
        white.addTags(new String[] { "b" });
        white.addAttributes("img", new String[] { "src", "width" });
        white.addAttributes("table", new String[] { "border", "cellspacing", "cellpadding" });
        white.addTags(new String[] { "tr" });
        white.addAttributes("td", new String[] { "width", "colspan", "rowspan" });
        String content = Jsoup.clean(str, white);
        return content;
    }

    /**
     *
     * */
    @Test
    public void getImg() throws Exception{
        Document doc = Jsoup.connect("http://www.yiibai.com").get();
        Elements images = doc.select("img[src~=(?i)\\.(png|jpe?g|gif)]");
        for (Element image : images) {
            System.out.println("src : " + image.attr("src"));
            System.out.println("height : " + image.attr("height"));
            System.out.println("width : " + image.attr("width"));
            System.out.println("alt : " + image.attr("alt"));
        }
    }

    @Test
    public void get() throws Exception{
        String html = "<p>An <a href='http://example.com/'><b>example</b></a> link.</p>";
        Document doc = Jsoup.parse(html);
        Element link = doc.select("a").first();
        System.out.println(link);
        String text = doc.body().text(); // "An example link"
        System.out.println(text);


        String linkHref = link.attr("href"); // "http://example.com/"
        System.out.println(linkHref);
        String linkText = link.text(); // "example""
        System.out.println(linkText);
        String linkOuterH = link.outerHtml();
        System.out.println(linkOuterH);
        // "<a href="http://example.com"><b>example</b></a>"
        String linkInnerH = link.html(); // "<b>example</b>"
        System.out.println(linkInnerH);
        link.hasClass("ssss");
        System.out.println("------------------------------");
        Document document = Jsoup.connect("http://jsoup.org").get();
        Element link__sun = document.select("a").first();
        String relHref = link__sun.attr("href"); // == "/"
        System.out.println(relHref);
        String absHref = link__sun.attr("abs:href"); // "http://jsoup.org/
        System.out.println(absHref);
        link__sun.attr("sss","ca");
        System.out.println(link__sun);
    }
}