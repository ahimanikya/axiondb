/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)EViewFunctionUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.axiondb.functions;

import org.axiondb.AxionException;

import com.sun.sbme.api.SbmeStandRecord;
import com.sun.sbme.api.SbmeStandRecordFactory;
import com.sun.sbme.api.SbmeStandardizationEngine;
import com.sun.sbmeapi.impl.StandConfigFilesAccessImpl;

/**
 * This class is just a wrapper for SbmeStandardizationEngine (EView)
 * 
 * @version  
 * @author Sudhi Seshachala
 */
public class EViewFunctionUtil {

    static {
        try {
            _instance = new EViewFunctionUtil();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private EViewFunctionUtil() throws Exception {
        _sbme = new SbmeStandardizationEngine(new StandConfigFilesAccessImpl());
    }

    public static EViewFunctionUtil getInstance() {
        return _instance;
    }

    public synchronized SbmeStandRecord standardize(String stdType, String addressColumn, String locale)
            throws AxionException {
        if (isNull(stdType)) {
            throw new AxionException("Standardization type 'Null' not allowed");
        }

        try {
            if (isNull(locale))
                locale = "US";
            return _sbme.standardize(stdType, addressColumn, locale)[0];
        } catch (Exception e) {
            throw new AxionException("While applying Standardization", e);
        }
    }

    private boolean isNull(String str) {
        return (str == null || str.trim().length() == 0);
    }

    public SbmeStandRecord normalize(String normalizeType, String normalizeConst,
            String normalizable, String locale) throws AxionException {

        if (isNull(normalizeType)) {
            throw new AxionException("Normalization type 'Null' not allowed");

        }
        try {
            SbmeStandRecord normalizableObj = SbmeStandRecordFactory.getInstance(normalizeType);
            normalizableObj.setValue(normalizeConst, normalizable);

            if (isNull(locale))
                locale = "US";
            return _sbme.normalize(normalizableObj, locale);
        } catch (Exception e) {
            throw new AxionException("While applying Normalization", e);
        }
    }

    private SbmeStandardizationEngine _sbme;
    private static EViewFunctionUtil _instance;
}
