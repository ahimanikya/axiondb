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
 * @(#)StandardizeFunction.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.axiondb.functions;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.functions.BaseFunction;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.ScalarFunction;
import org.axiondb.types.StringType;

/**
 * This supports EView functions
 *
 * @version  
 * @author Sudhi Seshachala
 */
public class StandardizeFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    // For Address Standardization
    // public static final String STRUCT_DESC = "WithinStructDescript";
    // public static final String STRUCT_ID = "WithinStructIdentif";
    // public static final String STREET_NAME_SUF_TYPE = "StreetNameSufType";
    // public static final String ORIG_STREET_NAME = "OrigStreetName";
    // public static final String MATCH_STREET_NAME = "MatchStreetName";
    // public static final String HOUSE_NUMBER = "HouseNumber";


    public StandardizeFunction() {
        super("Standardize");
    }

    private String convertToString(Object object) throws AxionException {
        return (String) RETURN_TYPE.convert(object);
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        Object val = null;

        Selectable sel1 = getArgument(0);
        Object stdType = sel1.evaluate(row);
        if (null == stdType) {
            throw new AxionException(
                "Standardization Type 'Address' or 'BusinessName' not specified");
        }

        Selectable sel2 = getArgument(1);
        Object partType = sel2.evaluate(row);
        if (null == partType) {
            throw new AxionException("Standardization identifier null for" + stdType
                + "Not supported");
        }

        Selectable sel3 = getArgument(2);
        Object col = sel3.evaluate(row);

        if (null == col) {
            return null;
        }
        DataType type = sel3.getDataType();
        val = type.convert(col);

        String locale = null;
        if (getArgumentCount() == 4) {
            Selectable sel4 = getArgument(3);
            locale = convertToString(sel4.evaluate(row));
        }


        Object obj = EViewFunctionUtil.getInstance().standardize(convertToString(stdType),
            convertToString(val), locale).getValue(convertToString(partType));


        return obj;
    }

    /**
     * @see org.axiondb.FunctionFactory#makeNewInstance()
     */
    public ConcreteFunction makeNewInstance() {
        return new StandardizeFunction();
    }

    public DataType getDataType() {
        return RETURN_TYPE;
    }

    public boolean isValid() {
        if (getArgumentCount() >= 3) {
            return true;
        }
        return false;
    }

    private static final DataType RETURN_TYPE = new StringType();
}
