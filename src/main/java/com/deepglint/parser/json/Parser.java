package com.deepglint.parser.json;

import java.util.LinkedList;
import java.util.TreeMap;

public class Parser {
    public static final char KEY = '@';
    public static final char INDEX = '#';
    private static final char NONE = '\u0000';
    private final StringBuilder fBuffer = new StringBuilder();
    private String iMessage;
    private int iIndex;
    private Element iRootElement;

    public Parser() {
    }

    public void parse(String aMessage) {
        assert aMessage != null;

        assert this.fBuffer != null;

        this.iMessage = aMessage;
        this.iIndex = 0;
        this.iRootElement = this.parseRoot();
    }

    public Element getRootElement() {
        assert this.iRootElement != null;

        return this.iRootElement;
    }

    public String toString() {
        assert this.iRootElement != null;

        return this.iRootElement.toString();
    }

    public boolean contains(Path aPath) {
        assert aPath != null;

        assert this.iRootElement != null;

        Element element = this.iRootElement;

        for (int i = 0; i < aPath.fDepth; ++i) {
            if (aPath.fKeys[i] != null) {
                element = element.getChildElement(aPath.fKeys[i]);
            } else {
                element = element.getChildElement(aPath.fIndexes[i]);
            }

            if (element == null) {
                return false;
            }
        }

        return true;
    }

    public boolean containsAll(Path[] aPaths) {
        assert aPaths != null;

        for (int i = 0; i < aPaths.length; ++i) {
            if (!this.contains(aPaths[i])) {
                return false;
            }
        }

        return true;
    }

    public boolean containsAny(Path[] aPaths) {
        assert aPaths != null;

        for (int i = 0; i < aPaths.length; ++i) {
            if (this.contains(aPaths[i])) {
                return true;
            }
        }

        return false;
    }

    public Element getElement(Path aPath) {
        assert aPath != null;

        assert this.iRootElement != null;

        Element element = this.iRootElement;

        for (int i = 0; i < aPath.fDepth; ++i) {
            if (aPath.fKeys[i] != null) {
                element = element.getChildElement(aPath.fKeys[i]);
            } else if(aPath.fIndexes[i] == -1){
                element = element.getChildElement(i+1, aPath);
                return element;
            } else {
                element = element.getChildElement(aPath.fIndexes[i]);
            }

            if (element == null) {
                return null;
            }
        }

        return element;
    }

    private String context() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        return this.iMessage.substring(0, this.iIndex) + " ^ " + this.iMessage.substring(this.iIndex);
    }

    private char next() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        return this.iIndex >= this.iMessage.length() ? '\u0000' : this.iMessage.charAt(this.iIndex++);
    }

    private char peek() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        return this.iIndex >= this.iMessage.length() ? '\u0000' : this.iMessage.charAt(this.iIndex);
    }

    private void back() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        if (this.iIndex != 0) {
            --this.iIndex;
        }
    }

    private void skip(int aSkip) {
        assert aSkip >= 0;

        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        this.iIndex += aSkip;
        if (this.iIndex >= this.iMessage.length()) {
            this.iIndex = this.iMessage.length();
        }

    }

    private String parseString() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        this.fBuffer.delete(0, this.fBuffer.length());
        char chr = this.next();

        assert chr == '"';

        chr = this.next();
        if (chr == '"') {
            return "";
        } else {
            while (chr != '"') {
                if (chr == 0) {
                    throw new RuntimeException("Parse failed during parse string element. Invalid syntax : " + this.context());
                }

                if (chr == '\\') {
                    this.fBuffer.append('\\');
                    chr = this.next();
                }

                this.fBuffer.append(chr);
                chr = this.next();
            }

            return this.fBuffer.toString();
        }
    }

    private String parseNumber() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        this.fBuffer.delete(0, this.fBuffer.length());
        char chr = this.next();

        assert "-0123456789".indexOf(chr) >= 0;

        do {
            if ("0123456789.Ee+-".indexOf(chr) < 0) {
                if ("]},".indexOf(chr) < 0) {
                    throw new RuntimeException("Parse failed during parse number element. Invalid syntax : " + this.context());
                }

                this.back();
                return this.fBuffer.toString();
            }

            this.fBuffer.append(chr);
            chr = this.next();
        } while (chr != 0);

        throw new RuntimeException("Parse failed during parse number element. Invalid syntax : " + this.context());
    }

    private String parseBoolean() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        char chr = this.next();

        assert "ft".indexOf(chr) >= 0;

        switch (chr) {
            case 'f':
                this.skip(4);
                return "false";
            case 't':
                this.skip(3);
                return "true";
            default:
                assert false;
                throw new RuntimeException("Parse failed during parse boolean element. Invalid syntax : " + this.context());
        }
    }

    private String parseNull() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        char chr = this.next();

        assert chr == 'n';

        this.skip(3);
        return "null";
    }

    private Element parseRoot() {
        assert this.iMessage != null;

        assert this.iIndex >= 0;

        assert this.iIndex <= this.iMessage.length();

        while (this.peek() <= ' ') {
            this.next();
        }

        switch (this.peek()) {
            case '[':
                return new ArrayElement(this.parseArray());
            case 'f':
                return new ScalarElement(this.parseBoolean());
            case 'n':
                return new ScalarElement(this.parseNull());
            case 't':
                return new ScalarElement(this.parseBoolean());
            case '{':
                return new ObjectElement(this.parseObject());
            default:
                throw new RuntimeException("Parse failed. Invalid syntax : " + this.context());
        }
    }

    private LinkedList<Element> parseArray() {
        LinkedList<Element> array = new LinkedList();
        char chr = this.next();

        assert chr == '[';

        while (chr != ']') {
            switch (this.peek()) {
                case '\t':
                case '\n':
                case '\r':
                case ' ':
                    chr = this.next();
                    break;
                case '"':
                    array.add(new ScalarElement(4, this.parseString()));
                    break;
                case ',':
                    chr = this.next();
                    break;
                case '-':
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    array.add(new ScalarElement(3, this.parseNumber()));
                    break;
                case '[':
                    array.add(new ArrayElement(this.parseArray()));
                    break;
                case ']':
                    chr = this.next();
                    break;
                case 'f':
                case 't':
                    array.add(new ScalarElement(2, this.parseBoolean()));
                    break;
                case 'n':
                    array.add(new ScalarElement(1, this.parseNull()));
                    break;
                case '{':
                    array.add(new ObjectElement(this.parseObject()));
                    break;
                default:
                    throw new RuntimeException("Invalid syntax : " + this.context());
            }
        }

        return array;
    }

    private TreeMap<String, Element> parseObject() {
        TreeMap<String, Element> object = new TreeMap();
        char chr = this.next();

        assert chr == '{';

        while (chr != '}') {
            switch (this.peek()) {
                case '\t':
                case '\n':
                case '\r':
                case ' ':
                    chr = this.next();
                    break;
                case '"':
                    String key = this.parseString();
                    while (this.peek() <= ' ') {
                        this.next();
                    }
                    chr = this.next();
                    if (chr != ':') {
                        throw new RuntimeException("Paser key failed. Invalid syntax : " + this.context());
                    }
                    while (this.peek() <= ' ') {
                        this.next();
                    }
                    switch (this.peek()) {
                        case '"':
                            object.put(key, new ScalarElement(4, this.parseString()));
                            continue;
                        case '-':
                        case '0':
                        case '1':
                        case '2':
                        case '3':
                        case '4':
                        case '5':
                        case '6':
                        case '7':
                        case '8':
                        case '9':
                            object.put(key, new ScalarElement(3, this.parseNumber()));
                            continue;
                        case '[':
                            object.put(key, new ArrayElement(this.parseArray()));
                            continue;
                        case 'f':
                        case 't':
                            object.put(key, new ScalarElement(2, this.parseBoolean()));
                            continue;
                        case 'n':
                            object.put(key, new ScalarElement(1, this.parseNull()));
                            continue;
                        case '{':
                            object.put(key, new ObjectElement(this.parseObject()));
                            continue;
                        default:
                            throw new RuntimeException("Parse failed. Invalid syntax : " + this.context());
                    }
                case ',':
                    chr = this.next();
                    break;
                case '}':
                    chr = this.next();
                    break;
                default:
                    throw new RuntimeException("Parse failed. Invalid syntax : " + this.context());
            }
        }

        return object;
    }
}
