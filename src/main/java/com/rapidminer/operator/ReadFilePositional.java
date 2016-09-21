package com.rapidminer.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.io.AbstractReader;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.Ontology;

public class ReadFilePositional extends AbstractReader<ExampleSet> {

	MemoryExampleTable table;
	int[] int_positions = null;

	public ReadFilePositional(OperatorDescription description) {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ExampleSet read() throws OperatorException {
		// TODO Auto-generated method stub
		String fileName = getParameterAsString("FILE_PATH");
		String partitions = getParameterAsString("POSITIONS");
		List<String> positions = Arrays.asList(partitions.split(","));
		int_positions = new int[positions.size()];
		// position.forEach(arg0);
		for (int i = 0; i < positions.size(); i++) {
			int_positions[i] = Integer.parseInt(positions.get(i));
		}
		String thisline = "";

		table = createTable();

		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

			stream.forEach(currentline -> splitandAdd(currentline));

			return table.createExampleSet();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private MemoryExampleTable createTable() {
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		for (int i = 0; i < int_positions.length - 1; i++) {
			Attribute attribute = AttributeFactory.createAttribute("column"
					+ String.valueOf(i), Ontology.POLYNOMINAL);
			attributes.add(attribute);

		}

		table = new MemoryExampleTable(attributes);
		return table;
	}

	private void splitandAdd(String currentline) {
		// attributes.get(attcounter).getMapping().mapString(field.getValue().toString());
		int numberOfColumns = int_positions.length - 1;
		double[] values = new double[numberOfColumns];

		for (int i = 0; i < int_positions.length - 1; i++) {
			// currentline.format(format, args)
			currentline = String.format("%-"
					+ int_positions[int_positions.length - 1] + "s",
					currentline);
			String currentValue = currentline.substring(int_positions[i],
					int_positions[i + 1]);
			values[i] = table.getAttribute(i).getMapping()
					.mapString(currentValue);

		}

		table.addDataRow(new DoubleArrayDataRow(values));
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		ParameterTypeString filepath = new ParameterTypeString("FILE_PATH",
				"Enter the PATH OF FILE");
		types.add(filepath);

		ParameterTypeString positions = new ParameterTypeString("POSITIONS",
				"Enter the positions");
		types.add(positions);

		return types;

	}

}