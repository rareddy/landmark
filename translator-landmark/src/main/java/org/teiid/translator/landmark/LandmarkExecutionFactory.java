/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.landmark;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DOUBLE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.SimpleJDBCExecutionFactory;

@Translator(name="landmark", description="LandMark Database Teiid Translator")
public class LandmarkExecutionFactory extends SimpleJDBCExecutionFactory {

	private static final String LANDMARK = "landmark"; //$NON-NLS-1$

	public LandmarkExecutionFactory() {
		setUseBindVariables(false);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$
        
        addPushDownFunction(LANDMARK, "to_degrees", DOUBLE, DOUBLE);//$NON-NLS-1$
        addPushDownFunction(LANDMARK, "to_radians", DOUBLE, DOUBLE);//$NON-NLS-1$
    } 
	
	
    @Override
    public List<String> getSupportedFunctions() {	
	    List<String> supportedFunctions = new ArrayList<String>();
	    supportedFunctions.addAll(super.getDefaultSupportedFunctions());
	    
	    supportedFunctions.add(SourceSystemFunctions.ABS);
	    supportedFunctions.add(SourceSystemFunctions.ACOS); 
	    supportedFunctions.add(SourceSystemFunctions.ASIN);
	    supportedFunctions.add(SourceSystemFunctions.ATAN);
	    supportedFunctions.add(SourceSystemFunctions.COS);
	    supportedFunctions.add(SourceSystemFunctions.EXP); 
	    supportedFunctions.add(SourceSystemFunctions.FLOOR);
	    supportedFunctions.add(SourceSystemFunctions.LOG);
	    supportedFunctions.add(SourceSystemFunctions.ROUND);
	    supportedFunctions.add(SourceSystemFunctions.SIN);
	    supportedFunctions.add(SourceSystemFunctions.SQRT);
	    supportedFunctions.add(SourceSystemFunctions.TAN);
	    supportedFunctions.add(SourceSystemFunctions.PI); 
	    supportedFunctions.add(SourceSystemFunctions.POWER);
	    return supportedFunctions;
    }
	
	@Override
    public boolean supportsAliasedTable() {
    	return true;
    }
	
	@Override
    public boolean useAsInGroupAlias(){
        return false;
    }	
	
    @Override
    public boolean supportsSelfJoins() {
        return true;
    }	
    
    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return true;
    }    
    
    @Override
    public boolean supportsGroupBy() {
    	return true;
    }    
    
    @Override
    public boolean supportsInCriteriaSubquery() {
        return false;
    }   
    
    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }    
    
    @Override
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }    
    
    @Override
    public boolean supportsInlineViews() {
        return false;
    }     
    
    @Override
    public boolean supportsUnions() {
        return false;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return false;
    }    
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return false;
    }    
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }   
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
    }

    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
    }    
    
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	obtainedConnection(conn);
    	return new LandmarkJDBCQueryExecution(command, conn, executionContext, this);
    }    
    
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
    	if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
    		return results.getTimestamp(columnIndex);
    	}
    	else if (expectedType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
    		return results.getDate(columnIndex);
    	}
    	else if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
    		return results.getTime(columnIndex);
    	}
    	return super.retrieveValue(results, columnIndex, expectedType);
    }
}
