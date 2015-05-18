var WordListModalCtrl = function ($scope, $modalInstance, name, words, mapping) {
    $scope.name = name;
    $scope.origName = name;
    $scope.errorMessage = "";
    $scope.newWord = "";
    $scope.words = words.slice(0); // create copy
    $scope.selectedWords = [];
    $scope.mapping = mapping;

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.addWord = function() {
        if ($scope.newWord) {
            $scope.words.push($scope.newWord);
            $scope.newWord = "";
        }
    };

    $scope.removeWord = function() {
        // sort the selected word indexes into descending order
        // so we can delete items without having to adjust indexes
        $scope.selectedWords.sort(function(a,b){ return b - a; });
        for (var index in $scope.selectedWords) {
            $scope.words.splice($scope.selectedWords[index], 1);
        }
        $scope.selectedWords = [];
    };

    $scope.build = function() {
        // must have a name
        if (!$scope.name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if ($scope.name != $scope.origName && $scope.mapping.analysis.token_maps[$scope.name]) {
            $scope.errorMessage = "Word list named '" + $scope.name + "' already exists";
            return;
        }

        result = {};
        result[$scope.name] = {
            "type": "custom",
            "tokens": $scope.words
        };
        $modalInstance.close(result);
    };
};