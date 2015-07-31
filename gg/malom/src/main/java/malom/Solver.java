package malom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;

public class Solver {

	public static void main(String[] args) throws Exception {

		System.out.println("VIGYAZAT! adjmasks atirva! (de csak felig, a can_close_mill-ben nem)");


		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);


		ArrayList<SectorId> sectors = new ArrayList<SectorId>();
		//sectors.add(new SectorId(3, 3, 0, 0));
		//sectors.add(new SectorId(1, 1, 0, 0));
		//sectors.add(new SectorId(2, 3, 0, 0)); sectors.add(new SectorId(3, 2, 0, 0));
		sectors.add(new SectorId(1, 3, 0, 0)); sectors.add(new SectorId(3, 1, 0, 0));

		//-Xmx6g -Xms6g

		Retrograde retr = new Retrograde(sectors, env);
		Graph<GameState, ValueCount, NullValue> res = retr.run();
		//res.getVertices().print();
		//System.out.println(res.getVertices().count());
		res.getVertices().writeAsText("/home/gabor/tmp/res.txt", FileSystem.WriteMode.OVERWRITE);
		env.execute();
	}
}
