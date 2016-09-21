package com.rapidminer.operator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.rapidminer.RapidMiner;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.io.AbstractReader;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeList;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.ParameterTypeStringCategory;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;
import com.rapidminer.tools.io.Encoding;

public class ReadFilePositional extends AbstractReader<ExampleSet> {

	MemoryExampleTable table;
	// int[] int_positions = null;
	List<String[]> pairs = null;
	boolean trimFlag = true;

	public ReadFilePositional(OperatorDescription description) {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ExampleSet read() throws OperatorException {
		// TODO Auto-generated method stub
		String fileName = getParameterAsString("FILE_PATH");
		// String partitions = getParameterAsString("POSITIONS");

		pairs = getParameterList("PARAMETER_COL_NAME_PAIRS");
		trimFlag = getParameterAsBoolean("PARAMETER_TRIM");

		String charsetName = getParameterAsString("PARAMETER_ENCODING");

		Charset charSet = Encoding.getEncoding(charsetName);

		// List<String> positions = Arrays.asList(partitions.split(","));
		// int_positions = new int[positions.size()];
		// position.forEach(arg0);
		// for (int i = 0; i < positions.size(); i++) {
		// int_positions[i] = Integer.parseInt(positions.get(i));
		// }
		String thisline = "";

		table = createTable();

		try (Stream<String> stream = Files.lines(Paths.get(fileName), charSet)) {

			stream.forEach(currentline -> splitandAdd(currentline));

			stream.close();
			return table.createExampleSet();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			// stream.close();
			e.printStackTrace();
		}
		return null;
	}

	private MemoryExampleTable createTable() {
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		for (int i = 0; i < pairs.size(); i++) {
			Attribute attribute = AttributeFactory.createAttribute(
					pairs.get(i)[0], Ontology.POLYNOMINAL);
			attributes.add(attribute);

		}

		table = new MemoryExampleTable(attributes);
		return table;
	}

	private int getMaxNumberofCharacters() {
		int maxvalue = 0;
		for (int i = 0; i < pairs.size() - 1; i++) {

			if (Integer.parseInt(pairs.get(i)[1].split(",")[1]) > maxvalue) {
				maxvalue = Integer.parseInt(pairs.get(i)[1].split(",")[1]);
			}
		}
		return maxvalue;
	}

	private void splitandAdd(String currentline) {

		int numberOfColumns = pairs.size();
		double[] values = new double[numberOfColumns];
		int maxvalue = getMaxNumberofCharacters();
		for (int i = 0; i < numberOfColumns; i++) {
			// todo pad to the max
			// currentline.format(format, args)
			currentline = String.format("%-" + maxvalue + "s", currentline);
			int startposition = Integer.parseInt(pairs.get(i)[1].split(",")[0]
					.trim());
			int endposition = Integer.parseInt(pairs.get(i)[1].split(",")[1]
					.trim());

			String currentValue = currentline.substring(startposition,
					endposition);
			if (trimFlag) {
				values[i] = table.getAttribute(i).getMapping()
						.mapString(currentValue.trim());

			} else {
				values[i] = table.getAttribute(i).getMapping()
						.mapString(currentValue);
			}

		}

		table.addDataRow(new DoubleArrayDataRow(values));
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		ParameterTypeString filepath = new ParameterTypeFile("FILE_PATH",
				"Enter the PATH OF FILE", "", false);
		types.add(filepath);

		// ParameterTypeString positions = new ParameterTypeString("POSITIONS",
		// "Enter the positions");
		// types.add(positions);

		ParameterTypeList type = new ParameterTypeList(
				"PARAMETER_COL_NAME_PAIRS", "Position of Columns.",
				new ParameterTypeString("PARAMETER_COLUMN_NAME",
						"The Column Name."), new ParameterTypeString(
						"PARAMETER_POSITION", "Start and end position.", false));

		types.add(type);

		final String[] CHARSETS = Encoding.CHARSETS;

		String encoding = RapidMiner.SYSTEM_ENCODING_NAME;
		String encodingProperty = ParameterService
				.getParameterValue(RapidMiner.PROPERTY_RAPIDMINER_GENERAL_DEFAULT_ENCODING);
		if (encodingProperty != null) {
			encoding = encodingProperty;
		}
		types.add(new ParameterTypeStringCategory("PARAMETER_ENCODING",
				"The encoding used for reading or writing files.", CHARSETS,
				encoding, false));

		types.add(new ParameterTypeBoolean("PARAMETER_TRIM",
				"Check to trim leading and trailing spaces", true, false));

		return types;

	}
}
