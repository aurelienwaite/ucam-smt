package uk.ac.cam.eng.rule.features;

public enum Feature {

	SOURCE2TARGET_PROBABILITY(Scope.GLOBAL, ComputeLocation.MAP_REDUCE),
	TARGET2SOURCE_PROBABILITY(Scope.GLOBAL, ComputeLocation.MAP_REDUCE),
	PROVENANCE_SOURCE2TARGET_PROBABILITY(Scope.PROVENANCE, ComputeLocation.MAP_REDUCE),
	PROVENANCE_TARGET2SOURCE_PROBABILITY(Scope.PROVENANCE, ComputeLocation.MAP_REDUCE),
	GLUE_RULE(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	INSERT_SCALE(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	SOURCE2TARGET_LEXICAL_PROBABILITY(Scope.GLOBAL, ComputeLocation.LEXICAL_SERVER),
	TARGET2SOURCE_LEXICAL_PROBABILITY(Scope.GLOBAL, ComputeLocation.LEXICAL_SERVER),
	PROVENANCE_SOURCE2TARGET_LEXICAL_PROBABILITY(Scope.PROVENANCE, ComputeLocation.LEXICAL_SERVER),
	PROVENANCE_TARGET2SOURCE_LEXICAL_PROBABILITY(Scope.PROVENANCE, ComputeLocation.LEXICAL_SERVER),
	RULE_COUNT_1(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	RULE_COUNT_2(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	RULE_COUNT_GREATER_THAN_2(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	RULE_INSERTION_PENALTY(Scope.GLOBAL, ComputeLocation.RETRIEVAL),
	WORD_INSERTION_PENALTY(Scope.GLOBAL, ComputeLocation.RETRIEVAL);
	
	/**
	 * Is the feature computed with respect to all training data (GLOBAL), or a subset (PROVENANCE)
	 * @author Aurelien Waite
	 *
	 */
	public static enum Scope{
		GLOBAL, PROVENANCE;
	}
	
	/**
	 * Where is this feature computed? During MapReduce or during retrieval time?
	 * All retrieval time features need to be associated with a lambda to compute
	 * them.
	 * @author Aurelien Waite
	 *
	 */
	public static enum ComputeLocation{
		MAP_REDUCE, RETRIEVAL, LEXICAL_SERVER;
	}
	
	public Scope scope;
	public ComputeLocation computed;
	
	private Feature(Scope scope, ComputeLocation comp){
		this.scope = scope;
		this.computed = comp;
	}
	
	public String getConfName() {
		return name().toLowerCase();
	}

	public static Feature findFromConf(String name) {
		return valueOf(name.toUpperCase());
	}
	
}