// controller responsible for building a custom analysis components

function AnalysisCtrl($scope, $http, $routeParams, $log, $sce, $location, $modal) {

    // analyzers

    $scope.newAnalyzer = function () {
        return $scope.editAnalyzer("", {
            "type": "custom",
            "char_filters": [],
            "tokenizer": "unicode",
            "token_filters": []
        });
    };

    $scope.deleteAnalyzer = function (name) {
        used = $scope.isAnalyzerUsed(name);
        if (used) {
            alert("This analyzer cannot be deleted because it is being used by the " + used + ".");
            return;
        }
        if (confirm("Are you sure you want to delete '" + name + "'?")) {
            delete $scope.$parent.mapping.analysis.analyzers[name];
        }
    };

    $scope.isAnalyzerUsed = function(name) {
        // analyzers are used in mappings (in various places)

        // first check index level default analyzer
        if ($scope.$parent.mapping.default_analyzer == name) {
            return "index mapping default analyzer";
        }

        // then check the default documnt mapping
        used = $scope.isAnalyzerUsedInDocMapping(name, $scope.$parent.mapping.default_mapping, "");
        if (used) {
            return "default document mapping " + used;
        }

        // then check the document mapping for each type
        for (var docType in $scope.$parent.mapping.types) {
            docMapping = $scope.$parent.mapping.types[docType];
            used = $scope.isAnalyzerUsedInDocMapping(name, docMapping, "");
            if (used) {
                return "document mapping type '" + docType + "' ";
            }
        }

        return null;
    };

    // a recursive helper
    $scope.isAnalyzerUsedInDocMapping = function(name, docMapping, path) {
        // first check the document level default analyzer
        if (docMapping.default_analyzer == name) {
            if (path) {
                return "default analyzer at " + path;
            } else {
                return "default analyzer";
            }
        }
        // now check fields at this level
        for (var fieldIndex in docMapping.fields) {
            field = docMapping.fields[fieldIndex];
            if (field.analyzer == name) {
                if (field.name) {
                    return "in the field named " + field.name;
                }
                return "in the field at path " + path;
            }
        }

        // now check each nested property
        for (var propertyName in docMapping.properties) {
            subDoc = docMapping.properties[propertyName];
            if (path) {
                return $scope.isAnalyzerUsedInDocMapping(name, subDoc, path+"."+propertyName);
            } else {
                return $scope.isAnalyzerUsedInDocMapping(name, subDoc, propertyName);
            }
        }

        return null;
    };

    $scope.editAnalyzer = function (name, value) {
        var modalInstance = $modal.open({
          animation: $scope.animationsEnabled,
          templateUrl: '/static/partials/analysis/analyzer.html',
          controller: 'AnalyzerModalCtrl',
          resolve: {
            name: function () {
              return name;
            },
            value: function () {
                return value;
            },
            mapping: function() {
                return $scope.$parent.mapping;
            }
          }
        });

        modalInstance.result.then(function (result) {
            // add this result to the mapping
            for (var resultKey in result) {
                if (name !== "" && resultKey != name) {
                    // remove the old name
                    delete $scope.$parent.mapping.analysis.analyzers[name];
                }
                $scope.$parent.mapping.analysis.analyzers[resultKey] = result[resultKey];
                // reload parent available analyzers
                $scope.$parent.loadAnalyzerNames();
            }
        }, function () {
          $log.info('Modal dismissed at: ' + new Date());
        });
    };

    // word lists

    $scope.newWordList = function () {
        return $scope.editWordList("", {tokens:[]});
    };

    $scope.deleteWordList = function (name) {
        used = $scope.isWordListUsed(name);
        if (used) {
            alert("This word list cannot be deleted because it is being used by the " + used + ".");
            return;
        }
        if (confirm("Are you sure you want to delete '" + name + "'?")) {
            delete $scope.$parent.mapping.analysis.token_maps[name];
        }
    };

    $scope.isWordListUsed = function(name) {
        // word lists are only used by token filters
        for (var tokenFilterName in $scope.$parent.mapping.analysis.token_filters) {
            tokenFilter = $scope.$parent.mapping.analysis.token_filters[tokenFilterName];
            // word lists are embeded in a variety of different field names
            if (tokenFilter.dict_token_map == name ||
                tokenFilter.articles_token_map == name ||
                tokenFilter.keywords_token_map == name ||
                tokenFilter.stop_token_map == name) {
                return "token filter named '" + tokenFilterName + "'";
            }
        }
        return null;
    };

	$scope.editWordList = function (name, value) {
        var modalInstance = $modal.open({
          animation: $scope.animationsEnabled,
          templateUrl: '/static/partials/analysis/wordlist.html',
          controller: 'WordListModalCtrl',
          resolve: {
            name: function () {
              return name;
            },
            words: function () {
                return value.tokens;
            },
            mapping: function() {
                return $scope.$parent.mapping;
            }
          }
        });

        modalInstance.result.then(function (result) {
            // add this result to the mapping
            for (var resultKey in result) {
                if (name !== "" && resultKey != name) {
                    // remove the old name
                    delete $scope.$parent.mapping.analysis.token_maps[name];
                }
                $scope.$parent.mapping.analysis.token_maps[resultKey] = result[resultKey];
            }
        }, function () {
          $log.info('Modal dismissed at: ' + new Date());
        });
    };

    // character filters

    $scope.newCharFilter = function() {
        return $scope.editCharFilter("", {});
    };

    $scope.deleteCharFilter = function(name) {
        used = $scope.isCharFilterUsed(name);
        if (used) {
            alert("This character filter cannot be deleted because it is being used by the " + used + ".");
            return;
        }
        if (confirm("Are you sure you want to delete '" + name + "'?")) {
            delete $scope.$parent.mapping.analysis.char_filters[name];
        }
    };

    $scope.isCharFilterUsed = function(name) {
        // character filters can only be used by analyzers
        for (var analyzerName in $scope.$parent.mapping.analysis.analyzers) {
            analyzer = $scope.$parent.mapping.analysis.analyzers[analyzerName];
            for (var charFilterIndex in analyzer.char_filters) {
                charFilterName = analyzer.char_filters[charFilterIndex];
                if (charFilterName == name) {
                    return "analyzer named '" + analyzerName + "'";
                }
            }
        }
        return null;
    };

    $scope.editCharFilter = function (name, value) {
        var modalInstance = $modal.open({
          animation: $scope.animationsEnabled,
          templateUrl: '/static/partials/analysis/charfilter.html',
          controller: 'CharFilterModalCtrl',
          resolve: {
            name: function () {
              return name;
            },
            value: function () {
                return value;
            },
            mapping: function() {
                return $scope.$parent.mapping;
            }
          }
        });

        modalInstance.result.then(function (result) {
            // add this result to the mapping
            for (var resultKey in result) {
                if (name !== "" && resultKey != name) {
                    // remove the old name
                    delete $scope.$parent.mapping.analysis.char_filters[name];
                }
                $scope.$parent.mapping.analysis.char_filters[resultKey] = result[resultKey];
            }
        }, function () {
          $log.info('Modal dismissed at: ' + new Date());
        });
    };

    // tokenizers

    $scope.newTokenizer = function () {
        return $scope.editTokenizer("", {});
    };

    $scope.deleteTokenizer = function (name) {
        used = $scope.isTokenizerUsed(name);
        if (used) {
            alert("This tokenizer cannot be deleted because it is being used by the " + used + ".");
            return;
        }
        if (confirm("Are you sure you want to delete '" + name + "'?")) {
            delete $scope.$parent.mapping.analysis.tokenizers[name];
        }
    };

    $scope.isTokenizerUsed = function(name) {
        // tokenizers can be used by *other* tokenizers
        for (var tokenizerName in $scope.$parent.mapping.analysis.tokenizers) {
            tokenizer = $scope.$parent.mapping.analysis.tokenizers[tokenizerName];
            if (tokenizer.tokenizer == name) {
                return "tokenizer named '" + tokenizerName + "'";
            }
        }

        // tokenizers can be used by analyzers
        for (var analyzerName in $scope.$parent.mapping.analysis.analyzers) {
            analyzer = $scope.$parent.mapping.analysis.analyzers[analyzerName];
            if (analyzer.tokenizer == name) {
                return "analyzer named '" + analyzerName + "'";
            }
        }
        return null;
    };

    $scope.editTokenizer = function (name, value) {
        var modalInstance = $modal.open({
          animation: $scope.animationsEnabled,
          templateUrl: '/static/partials/analysis/tokenizer.html',
          controller: 'TokenizerModalCtrl',
          resolve: {
            name: function () {
              return name;
            },
            value: function () {
                return value;
            },
            mapping: function() {
                return $scope.$parent.mapping;
            }
          }
        });

        modalInstance.result.then(function (result) {
            // add this result to the mapping
            for (var resultKey in result) {
                if (name !== "" && resultKey != name) {
                    // remove the old name
                    delete $scope.$parent.mapping.analysis.tokenizers[name];
                }
                $scope.$parent.mapping.analysis.tokenizers[resultKey] = result[resultKey];
            }
        }, function () {
          $log.info('Modal dismissed at: ' + new Date());
        });
    };

    // token filters

    $scope.newTokenFilter = function () {
        return $scope.editTokenFilter("", {});
    };

    $scope.deleteTokenFilter = function (name) {
        used = $scope.isTokenFilterUsed(name);
        if (used) {
            alert("This token filter cannot be deleted because it is being used by the " + used + ".");
            return;
        }
        if (confirm("Are you sure you want to delete '" + name + "'?")) {
            delete $scope.$parent.mapping.analysis.token_filters[name];
        }
    };

    $scope.isTokenFilterUsed = function(name) {
        // token filters can only be used by analyzers
        for (var analyzerName in $scope.$parent.mapping.analysis.analyzers) {
            analyzer = $scope.$parent.mapping.analysis.analyzers[analyzerName];
            for (var tokenFilterIndex in analyzer.token_filters) {
                tokenFilterName = analyzer.token_filters[tokenFilterIndex];
                if (tokenFilterName == name) {
                    return "analyzer named '" + analyzerName + "'";
                }
            }
        }
        return null;
    };

    $scope.editTokenFilter = function (name, value) {
        var modalInstance = $modal.open({
          animation: $scope.animationsEnabled,
          templateUrl: '/static/partials/analysis/tokenfilter.html',
          controller: 'TokenFilterModalCtrl',
          resolve: {
            name: function () {
              return name;
            },
            value: function () {
                return value;
            },
            mapping: function() {
                return $scope.$parent.mapping;
            }
          }
        });

        modalInstance.result.then(function (result) {
            // add this result to the mapping
            for (var resultKey in result) {
                if (name !== "" && resultKey != name) {
                    // remove the old name
                    delete $scope.$parent.mapping.analysis.token_filters[name];
                }
                $scope.$parent.mapping.analysis.token_filters[resultKey] = result[resultKey];
            }
        }, function () {
          $log.info('Modal dismissed at: ' + new Date());
        });
    };

}