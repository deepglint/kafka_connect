package com.deepglint.parser.json;

public class Path {
    public static final char KEY = '@';
    public static final char INDEX = '#';
    protected final char fSeparator;
    protected final int fDepth;
    protected final String[] fKeys;
    protected final int[] fIndexes;

    public Path(String aString) {
        assert aString != null;

        if (aString.length() < 2) {
            throw new RuntimeException("Empty path : " + aString);
        } else {
            this.fSeparator = aString.charAt(0);
            String[] tags = aString.substring(1).split("\\" + this.fSeparator, -1);
            this.fDepth = tags.length;
            this.fKeys = new String[this.fDepth];
            this.fIndexes = new int[this.fDepth];

            for(int i = 0; i < this.fDepth; ++i) {
                if (tags[i].trim().length() < 2) {
                    throw new RuntimeException("Bad tag : " + aString + " [" + i + "]");
                }

                if (tags[i].charAt(0) == '@') {
                    this.fKeys[i] = tags[i].substring(1);

                    assert this.fKeys[i] != null;
                } else {
                    if (tags[i].charAt(0) != '#') {
                        throw new RuntimeException("Bad tag : " + aString + " [" + i + "]");
                    }

                    for(int j = 1; j < tags[i].length(); ++j) {
                        if ("-0123456789".indexOf(tags[i].charAt(j)) < 0) {
                            throw new RuntimeException("Bad tag : " + aString + " [" + i + "]");
                        }
                    }
                    try{
                        this.fIndexes[i] = Integer.parseInt(tags[i].substring(1));
                    }catch (NumberFormatException e){
                        e.printStackTrace();
                    }
                }
            }

        }
    }

    public String toString() {
        assert this.fDepth > 0;

        StringBuilder buffer = new StringBuilder();

        for(int i = 0; i < this.fDepth; ++i) {
            buffer.append(this.fSeparator);
            if (this.fKeys[i] != null) {
                buffer.append('@');
                buffer.append(this.fKeys[i]);
            } else {
                buffer.append('#');
                buffer.append(this.fIndexes[i]);
            }
        }

        return buffer.toString();
    }

    public static String getTag(Element aElement) {
        assert aElement != null;

        if (aElement.iParent == null) {
            return "";
        } else {
            return aElement.iKey != null ? '@' + aElement.iKey : '#' + String.valueOf(aElement.iIndex);
        }
    }

    public static String getPath(Element aElement, char aSeparator) {
        assert aElement != null;

        assert aSeparator > 0;

        StringBuilder stringBuilder = new StringBuilder();

        for(Element element = aElement; element.iParent != null; element = element.iParent) {
            stringBuilder.insert(0, getTag(element));
            stringBuilder.insert(0, aSeparator);
        }

        return stringBuilder.toString();
    }
}
