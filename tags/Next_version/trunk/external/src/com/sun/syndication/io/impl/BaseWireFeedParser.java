package com.sun.syndication.io.impl;

import com.sun.syndication.feed.WireFeed;
import com.sun.syndication.feed.module.Extendable;
import com.sun.syndication.io.WireFeedParser;
import java.util.ArrayList;
import java.util.Iterator;
import org.jdom.Element;

import java.util.List;
import org.jdom.Namespace;

/**
 * @author Alejandro Abdelnur
 */
public abstract class BaseWireFeedParser implements WireFeedParser {
    /**
     * [TYPE].feed.ModuleParser.classes=  [className] ...
     *
     */
    private static final String FEED_MODULE_PARSERS_POSFIX_KEY = ".feed.ModuleParser.classes";

    /**
     * [TYPE].item.ModuleParser.classes= [className] ...
     *
     */
    private static final String ITEM_MODULE_PARSERS_POSFIX_KEY = ".item.ModuleParser.classes";


    private String _type;
    private ModuleParsers _feedModuleParsers;
    private ModuleParsers _itemModuleParsers;

    protected BaseWireFeedParser(String type) {
        _type = type;
        _feedModuleParsers = new ModuleParsers(type+FEED_MODULE_PARSERS_POSFIX_KEY, this);
        _itemModuleParsers = new ModuleParsers(type+ITEM_MODULE_PARSERS_POSFIX_KEY, this);
    }

    /**
     * Returns the type of feed the parser handles.
     * <p>
     * @see WireFeed for details on the format of this string.
     * <p>
     * @return the type of feed the parser handles.
     *
     */
    public String getType() {
        return _type;
    }

    protected List parseFeedModules(Element feedElement) {
        return _feedModuleParsers.parseModules(feedElement);
    }

    protected List parseItemModules(Element itemElement) {
        return _itemModuleParsers.parseModules(itemElement);
    }
    
    protected List extractForeignMarkup(Element e, Extendable ext, Namespace basens) {
        ArrayList foreignMarkup = new ArrayList();
        Iterator children = e.getChildren().iterator();
        while (children.hasNext()) {
            Element elem = (Element)children.next();
            if  ( 
               // if elemet not in the RSS namespace
               !basens.equals(elem.getNamespace())
               // and elem was not handled by a module
               && null == ext.getModule(elem.getNamespaceURI())) {

               // save it as foreign markup, 
               // but we can't detach it while we're iterating
               foreignMarkup.add(elem.clone()); 
            }
        }
        // Now we can detach the foreign markup elements
        Iterator fm = foreignMarkup.iterator();
        while (fm.hasNext()) {
            Element elem = (Element)fm.next();
            elem.detach();
        }
        return foreignMarkup;
    }
}

