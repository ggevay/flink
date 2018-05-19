package malom;

import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

//-Xmx4g -Xms4g

public class Solver {

	static final boolean REUSE = false; // influences both the config opt and when emitting from UDFs

	public static void main(String[] args) throws Exception {

		System.out.println("VIGYAZAT! adjmasks atirva!");
		//System.out.println("VIGYAZAT! lose condition atirva!");


		Config.outPath = args[0];

		SectorId rootSector = new SectorId(Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]));


		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(1);

		if (REUSE) {
			env.getConfig().enableObjectReuse();
		} else {
			env.getConfig().disableObjectReuse();
		}

		PojoTypeInfo.registerCustomSerializer(GameState.class, GameState.GameStateSerializer.class);
		PojoTypeInfo.registerCustomSerializer(ValueCount.class, ValueCount.ValueCountSerializer.class);
		PojoTypeInfo.registerCustomSerializer(Value.class, Value.ValueSerializer.class);

		Config.combineHint = CombineHint.HASH;



//		Retrograde retrograde = new Retrograde(env);
//		retrograde.solve(rootSector);

//		RetrogradeWithoutGelly retrograde = new RetrogradeWithoutGelly();
//		retrograde.solve(rootSector, env);

		RetrogradeWithoutGellyUnioned.solve(rootSector, env);


		long start = System.currentTimeMillis();

		env.execute();
		//System.out.println(env.getExecutionPlan());

		long end = System.currentTimeMillis();
		System.out.println("time: " + (end - start) + "ms");
	}
}
