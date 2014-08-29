function SearchCtrl($scope, $http, $routeParams, $log, $sce) {

    $scope.inclusiveMin = true;
    $scope.inclusiveMax = false;
    $scope.inclusiveStart = true;
    $scope.inclusiveEnd = false;
    $scope.fieldNames = [];

    $scope.minShouldOptions = [];
    for (var i = 0; i <= 50; i++) {
            $scope.minShouldOptions[i] = i;
    }

    var resetSchema = function() {
        $scope.phraseTerm = "";
        $scope.clauseTerm = "";
        $scope.clauseOccur = "MUST";
        $scope.clauseBoost = 1.0;
    };

    var resetForm = function() {
            $scope.clauses = [];
            $scope.phraseTerms = [];
            $scope.size = "10";
            $scope.minShould = "0";
            resetSchema();
    };

    resetForm();

    updateFieldNames = function() {
        $http.get('/api/fields').success(function(data) {
            $scope.fieldNames = data.fields;
        }).
        error(function(data, code) {

        });
    };

    updateFieldNames();

    $scope.searchTerm = function() {
        $http.post('/api/search', {
			"size": 10,
			"explain": true,
			"highlight":{},
			"query": {
				"term": $scope.term,
				"field": $scope.field,
			}
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchPrefix = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "prefix": $scope.prefix,
                "field": $scope.field,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchNumericRange = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "min": parseFloat($scope.min),
                "max": parseFloat($scope.max),
                "inclusive_min": $scope.inclusiveMin,
                "inclusive_max": $scope.inclusiveMax,
                "field": $scope.field,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchDateRange = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "start": $scope.startDate,
                "end": $scope.endDate,
                "inclusive_start": $scope.inclusiveStart,
                "inclusive_end": $scope.inclusiveEnd,
                "field": $scope.field,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchMatch = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "boost": 1.0,
                "match": $scope.match,
                "field": $scope.field,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchMatchPhrase = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "boost": 1.0,
                "match_phrase": $scope.matchphrase,
                "field": $scope.field,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.searchSyntax = function() {
        $http.post('/api/search', {
            "size": 10,
            "explain": true,
            "highlight":{},
            "query": {
                "boost": 1.0,
                "query": $scope.syntax,
            }
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.expl = function(explanation) {
            rv = "" + $scope.roundScore(explanation.value) + " - " + explanation.message;
            rv = rv + "<ul>";
            for(var i in explanation.children) {
                    child = explanation.children[i];
                    rv = rv + "<li>" + $scope.expl(child) + "</li>";
            }
            rv = rv + "</ul>";
            return rv;
    };

    $scope.roundScore = function(score) {
            return Math.round(score*1000)/1000;
    };

    $scope.roundTook = function(took) {
        if (took < 1000 * 1000) {
            return "less than 1ms";
        } else if (took < 1000 * 1000 * 1000) {
            return "" + Math.round(took / (1000*1000)) + "ms";
        } else {
            roundMs = Math.round(took / (1000*1000));
            return "" + roundMs/1000 + "s";
        }
	};

    $scope.removePhraseTerm = function(index) {
        $scope.phraseTerms.splice(index, 1);
    };

    $scope.addPhraseTerm = function() {
        if($scope.phraseTerm === "") {
                //$scope.errorMessage = "Phrase term cannot be empty";
                //return;
                $scope.phraseTerms.push(null);
        }else {

        $scope.phraseTerms.push($scope.phraseTerm);
    }

        // reset form
        delete $scope.errorMessage;
        resetSchema();
    };

    $scope.processResults = function(data) {
        $scope.errorMessage = null;
        $scope.results = data;
        for(var i in $scope.results.hits) {
                hit = $scope.results.hits[i];
                hit.roundedScore = $scope.roundScore(hit.score);
                hit.explanationString = $scope.expl(hit.explanation);
                hit.explanationStringSafe = $sce.trustAsHtml(hit.explanationString);
                for(var ff in hit.fragments) {
                    fragments = hit.fragments[ff];
                    newFragments = [];
                    for(var ffi in fragments) {
                        fragment = fragments[ffi];
                        safeFragment = $sce.trustAsHtml(fragment);
                        newFragments.push(safeFragment);
                    }
                    hit.fragments[ff] = newFragments;
                }
        }
        $scope.results.roundTook = $scope.roundTook(data.took);
    };

    $scope.searchPhrase = function() {
        delete $scope.results;
        if($scope.phraseTerms.length < 1) {
                $scope.errorMessage = "Query requires at least one term";
                return;
        }
        var requestBody = {
                "query": {
                        "terms": [],
                        "boost": 1.0,
                },
                "highlight":{},
                explain: true,
                size: parseInt($scope.size, 10)
        };
        for(var i in $scope.phraseTerms) {
                var term = $scope.phraseTerms[i];
                if (term !== null) {
                    var termQuery = {
                        "term": term,
                        "field": $scope.phraseField,
                        "boost": 1.0,
                    };
                    requestBody.query.terms.push(termQuery);
                } else {
                    requestBody.query.terms.push(null);
                }

        }

        $http.post('/api/search', requestBody).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {
                $scope.errorMessage = data;
                return;
        });
    };

    $scope.removeClause = function(index) {
        $scope.clauses.splice(index, 1);
    };

    $scope.addClause = function() {

        if($scope.clauseTerm === "") {
                $scope.errorMessage = "Clause term cannot be empty";
                return;
        }

        if($scope.clauseOccur === "") {
                $scope.errorMessage = "Select clause occur";
                return;
        }

        if($scope.clauseField === "") {
                $scope.errorMessage = "Select a field";
                return;
        }

        if($scope.clauseBoost === "") {
                $scope.errorMessage = "Clause boost cannot be empty";
                return;
        }

        clause = {
                "term": $scope.clauseTerm,
                "occur": $scope.clauseOccur,
                "field": $scope.clauseField,
                "boost": $scope.clauseBoost
        };

        $scope.clauses.push(clause);

        // reset form
        delete $scope.errorMessage;
        resetSchema();
    };

    $scope.searchBoolean = function() {
        delete $scope.results;
        if($scope.clauses.length < 1) {
                $scope.errorMessage = "Query requires at least one clause";
                return;
        }
        var requestBody = {
                "query": {
                        "must": {
                                "conjuncts":[],
                                "boost": 1.0,
                        },
                        "should":{
                                "disjuncts":[],
                                "boost": 1.0,
                                "min": parseInt($scope.minShould, 10)
                        },
                        "must_not": {
                                "disjuncts": [],
                                "boost": 1.0,
                        },
                        "boost": 1.0,
                },
                explain: true,
                "highlight":{},
                size: parseInt($scope.size, 10)
        };
        for(var i in $scope.clauses) {
                var clause = $scope.clauses[i];
                var termQuery = {
                        "term": clause.term,
                        "field": clause.field,
                        "boost": parseFloat(clause.boost),
                };
                switch(clause.occur) {
                        case "MUST":
                        requestBody.query.must.conjuncts.push(termQuery);
                        break;
                        case "SHOULD":
                        requestBody.query.should.disjuncts.push(termQuery);
                        break;
                        case "MUST NOT":
                        requestBody.query.must_not.disjuncts.push(termQuery);
                        break;
                }
        }
        if (requestBody.query.must.conjuncts.length === 0) {
                delete requestBody.query.must;
        }
        if (requestBody.query.should.disjuncts.length === 0) {
                delete requestBody.query.should;
        }
        if (requestBody.query.must_not.disjuncts.length === 0) {
                delete requestBody.query.must_not;
        }

        $http.post('/api/search', requestBody).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {
                $scope.errorMessage = data;
                return;
        });
    };

}