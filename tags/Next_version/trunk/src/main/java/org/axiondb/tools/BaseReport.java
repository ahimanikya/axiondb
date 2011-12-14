/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The names "Tigris", "Axion", nor the names of its contributors may
 *    not be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 *
 * 4. Products derived from this software may not be called "Axion", nor
 *    may "Tigris" or "Axion" appear in their names without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * =======================================================================
 */
package org.axiondb.tools;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Girish Patil 
 * Utility class for Console and Batch command runner to format the output.
 */
public class BaseReport {
	public BaseReport(PrintWriter pw) {
		this._writer = pw;
	}

	public void reportException(Exception e) {
		_writer.println(e.toString());
	}

	public void reportUpdateCount(int count) {
		switch (count) {
		case -1:
			_writer.println("Executed.");
			break;
		case 0:
			_writer.println("No rows changed.");
			break;
		case 1:
			_writer.println("1 row changed.");
			break;
		default:
			_writer.println(count + " rows changed.");
			break;
		}
	}

	public void reportResultSet(ResultSet rset) throws SQLException {
		if (rset != null) {
			ResultSetMetaData meta = rset.getMetaData();
			int count = meta.getColumnCount();
			int[] colWidths = new int[count];
			List[] colValues = new List[count];
			for (int i = 0; i < count; i++) {
				String label = meta.getColumnLabel(i + 1);
				if (label != null) {
					colWidths[i] = label.length();
				}
				colValues[i] = new ArrayList();
			}
			// find column values and widths
			while (rset.next()) {
				for (int i = 0; i < count; i++) {
					String val = rset.getString(i + 1);
					if (rset.wasNull()) {
						val = "NULL";
					}
					if (val.length() > colWidths[i]) {
						colWidths[i] = val.length();
					}
					colValues[i].add(val);
				}
			}
			// table header
			printBoundary('=', colWidths);
			for (int i = 0; i < count; i++) {
				String label = meta.getColumnLabel(i + 1);
				_writer.print("|");
				printCentered(label, colWidths[i]);
			}
			_writer.println("|");
			printBoundary('=', colWidths);
			// values
			for (int i = 0, I = colValues[0].size(); i < I; i++) {
				// for each row
				for (int j = 0; j < count; j++) {
					// for each column
					String value = (String) colValues[j].get(i);
					_writer.print("|");
					printRight(value, colWidths[j]);
				}
				_writer.println("|");
				printBoundary('-', colWidths);
			}
		}
	}

	public void printBoundary(char boundaryChar, int[] colWidths) {
		for (int i = 0; i < colWidths.length; i++) {
			_writer.print("+");
			for (int j = 0; j < colWidths[i] + 2; j++) {
				_writer.print(boundaryChar);
			}
		}
		_writer.println("+");
	}

	public void printCentered(String value, int length) {
		_writer.print(" ");
		int diff = length - value.length();
		int left = diff / 2;
		int right = left;
		if (diff % 2 == 1) {
			left++;
		}
		for (int j = 0; j < left; j++) {
			_writer.print(" ");
		}
		_writer.print(value);
		for (int j = 0; j < right; j++) {
			_writer.print(" ");
		}
		_writer.print(" ");
	}

	public void printRight(String value, int length) {
		_writer.print(" ");
		int diff = length - value.length();
		for (int j = 0; j < diff; j++) {
			_writer.print(" ");
		}
		_writer.print(value);
		_writer.print(" ");
	}

	private PrintWriter _writer;
}

