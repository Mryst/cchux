package com.cchux.report.controller;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class TestController {

    @RequestMapping("/test")
    public String test(String json){
        System.out.println(json+"\ttest over");
        return json;
    }

}
