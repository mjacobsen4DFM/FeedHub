package com.DFM.Model.WordPress;

import javax.xml.bind.annotation.XmlElement;
import java.io.Serializable;

public class Image implements Serializable {
    private static final long serialVersionUID = -5913957382972359157L;
    private String source;
    private String guid;
    private String name;
    private String caption;
    private String credit;
    private String mimetype;
    private String title;

    public String getSource() {
        return source;
    }

    @XmlElement
    public void setSource(String source) {
        this.source = source;
    }

    public String getName() {
        return name;
    }

    @XmlElement
    public void setName(String name) {
        this.name = name;
    }

    public String getCaption() {
        return caption;
    }

    @XmlElement
    public void setCaption(String caption) {
        this.caption = caption;
    }

    public String getCredit() {
        return credit;
    }

    @XmlElement
    public void setCredit(String credit) {
        this.credit = credit;
    }

    public String getMimetype() {
        return mimetype;
    }

    @XmlElement
    public void setMimetype(String mimetype) {
        this.mimetype = mimetype;
    }

    public String getTitle() {
        return title;
    }

    @XmlElement
    public void setTitle(String title) {
        this.title = title;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }
}
