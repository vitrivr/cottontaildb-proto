package org.vitrivr.cottontail.client.language.basics

/*
* An enumeration of all [Distances] supported by Cottontail DB for NNS.
*
* @author Ralph Gasser
* @version 1.2.0
*/
enum class Distances(val functionName: String) {
    L1("manhattan"),
    MANHATTAN("manhattan"),
    L2("euclidean"),
    EUCLIDEAN("euclidean"),
    SQUAREDEUCLIDEAN("squaredeuclidean"),
    L2SQUARED("squaredeuclidean"),
    HAMMING("hamming"),
    COSINE("cosine"),
    CHI2("chisquared"),
    CHISQUARED("chisquared"),
    IP("innerproduct"),
    INNERPRODUCT("innerproduct"),
    DOTP("innerproduct"),
    HAVERSINE("haversine");
}