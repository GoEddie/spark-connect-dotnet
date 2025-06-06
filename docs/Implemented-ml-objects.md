## Spark ML Objects

Note that the current implementation is not complete and is subject to change.

| Type        | Class                                                                      | Implemented |
|-------------|----------------------------------------------------------------------------|-------------|
| Transformer | org.apache.spark.ml.classification.NaiveBayesModel                         | Yes         |
| Transformer | org.apache.spark.ml.classification.GBTClassificationModel                  |             |
| Transformer | org.apache.spark.ml.feature.Word2VecModel                                  | Yes         |
| Transformer | org.apache.spark.ml.feature.CountVectorizerModel                           |             |
| Transformer | org.apache.spark.ml.feature.TargetEncoderModel                             |             |
| Transformer | org.apache.spark.ml.classification.LinearSVCModel                          |             |
| Transformer | org.apache.spark.ml.feature.OneHotEncoderModel                             |             |
| Transformer | org.apache.spark.ml.feature.StandardScalerModel                            |             |
| Transformer | org.apache.spark.ml.feature.RFormulaModel                                  |             |
| Transformer | org.apache.spark.ml.feature.Binarizer                                      | Yes         |
| Transformer | org.apache.spark.ml.clustering.LocalLDAModel                               |             |
| Transformer | org.apache.spark.ml.feature.VectorSlicer                                   |             |
| Transformer | org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel |             |
| Transformer | org.apache.spark.ml.feature.DCT                                            |             |
| Transformer | org.apache.spark.ml.feature.ElementwiseProduct                             |             |
| Transformer | org.apache.spark.ml.feature.Bucketizer                                     | Yes         |
| Transformer | org.apache.spark.ml.feature.StopWordsRemover                               |             |
| Transformer | org.apache.spark.ml.feature.PCAModel                                       |             |
| Transformer | org.apache.spark.ml.clustering.GaussianMixtureModel                        |             |
| Transformer | org.apache.spark.ml.regression.GeneralizedLinearRegressionModel            |             |
| Transformer | org.apache.spark.ml.regression.AFTSurvivalRegressionModel                  |             |
| Transformer | org.apache.spark.ml.clustering.BisectingKMeansModel                        |             |
| Transformer | org.apache.spark.ml.clustering.KMeansModel                                 |             |
| Transformer | org.apache.spark.ml.feature.MaxAbsScalerModel                              |             |
| Transformer | org.apache.spark.ml.regression.FMRegressionModel                           |             |
| Transformer | org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel               |             |
| Transformer | org.apache.spark.ml.classification.LogisticRegressionModel                 | Yes         |
| Transformer | org.apache.spark.ml.regression.DecisionTreeRegressionModel                 |             |
| Transformer | org.apache.spark.ml.feature.Normalizer                                     |             |
| Transformer | org.apache.spark.ml.classification.RandomForestClassificationModel         |             |
| Transformer | org.apache.spark.ml.recommendation.ALSModel                                |             |
| Transformer | org.apache.spark.ml.feature.VectorAssembler                                | Yes         |
| Transformer | org.apache.spark.ml.regression.IsotonicRegressionModel                     |             |
| Transformer | org.apache.spark.ml.classification.DecisionTreeClassificationModel         |             |
| Transformer | org.apache.spark.ml.feature.MinMaxScalerModel                              |             |
| Transformer | org.apache.spark.ml.feature.ImputerModel                                   |             |
| Transformer | org.apache.spark.ml.feature.VectorSizeHint                                 |             |
| Transformer | org.apache.spark.ml.feature.Interaction                                    |             |
| Transformer | org.apache.spark.ml.fpm.FPGrowthModel                                      |             |
| Transformer | org.apache.spark.ml.feature.FeatureHasher                                  |             |
| Transformer | org.apache.spark.ml.clustering.PowerIterationClusteringWrapper             |             |
| Transformer | org.apache.spark.ml.feature.SQLTransformer                                 |             |
| Transformer | org.apache.spark.ml.feature.IDFModel                                       |             |
| Transformer | org.apache.spark.ml.feature.UnivariateFeatureSelectorModel                 |             |
| Transformer | org.apache.spark.ml.feature.PolynomialExpansion                            |             |
| Transformer | org.apache.spark.ml.feature.VarianceThresholdSelectorModel                 |             |
| Transformer | org.apache.spark.ml.feature.ChiSqSelectorModel                             |             |
| Transformer | org.apache.spark.ml.feature.IndexToString                                  |             |
| Transformer | org.apache.spark.ml.feature.RobustScalerModel                              |             |
| Transformer | org.apache.spark.ml.regression.GBTRegressionModel                          |             |
| Transformer | org.apache.spark.ml.clustering.DistributedLDAModel                         |             |
| Transformer | org.apache.spark.ml.feature.NGram                                          |             |
| Transformer | org.apache.spark.ml.feature.StringIndexerModel                             |             |
| Transformer | org.apache.spark.ml.feature.HashingTF                                      |             |
| Transformer | org.apache.spark.ml.feature.VectorIndexerModel                             |             |
| Transformer | org.apache.spark.ml.feature.Tokenizer                                      | Yes         |
| Transformer | org.apache.spark.ml.feature.MinHashLSHModel                                |             |
| Transformer | org.apache.spark.ml.regression.LinearRegressionModel                       |             |
| Transformer | org.apache.spark.ml.regression.RandomForestRegressionModel                 |             |
| Transformer | org.apache.spark.ml.feature.RegexTokenizer                                 |             |
| Transformer | org.apache.spark.ml.classification.FMClassificationModel                   |             |
| Estimator   | org.apache.spark.ml.classification.DecisionTreeClassifier                  |             |
| Estimator   | org.apache.spark.ml.regression.IsotonicRegression                          |             |
| Estimator   | org.apache.spark.ml.fpm.FPGrowth                                           |             |
| Estimator   | org.apache.spark.ml.feature.CountVectorizer                                |             |
| Estimator   | org.apache.spark.ml.feature.MinMaxScaler                                   |             |
| Estimator   | org.apache.spark.ml.regression.FMRegressor                                 |             |
| Estimator   | org.apache.spark.ml.feature.MaxAbsScaler                                   |             |
| Estimator   | org.apache.spark.ml.regression.GBTRegressor                                |             |
| Estimator   | org.apache.spark.ml.regression.RandomForestRegressor                       |             |
| Estimator   | org.apache.spark.ml.feature.TargetEncoder                                  |             |
| Estimator   | org.apache.spark.ml.regression.GeneralizedLinearRegression                 |             |
| Estimator   | org.apache.spark.ml.feature.MinHashLSH                                     |             |
| Estimator   | org.apache.spark.ml.feature.VarianceThresholdSelector                      |             |
| Estimator   | org.apache.spark.ml.clustering.LDA                                         |             |
| Estimator   | org.apache.spark.ml.classification.LogisticRegression                      | Yes         |
| Estimator   | org.apache.spark.ml.classification.LinearSVC                               |             |
| Estimator   | org.apache.spark.ml.regression.DecisionTreeRegressor                       |             |
| Estimator   | org.apache.spark.ml.feature.VectorIndexer                                  |             |
| Estimator   | org.apache.spark.ml.feature.Word2Vec                                       | Yes         |
| Estimator   | org.apache.spark.ml.feature.IDF                                            |             |
| Estimator   | org.apache.spark.ml.classification.GBTClassifier                           |             |
| Estimator   | org.apache.spark.ml.classification.RandomForestClassifier                  |             |
| Estimator   | org.apache.spark.ml.classification.MultilayerPerceptronClassifier          |             |
| Estimator   | org.apache.spark.ml.feature.OneHotEncoder                                  |             |
| Estimator   | org.apache.spark.ml.clustering.BisectingKMeans                             |             |
| Estimator   | org.apache.spark.ml.regression.LinearRegression                            |             |
| Estimator   | org.apache.spark.ml.feature.StringIndexer                                  |             |
| Estimator   | org.apache.spark.ml.clustering.GaussianMixture                             |             |
| Estimator   | org.apache.spark.ml.feature.Imputer                                        |             |
| Estimator   | org.apache.spark.ml.clustering.KMeans                                      |             |
| Estimator   | org.apache.spark.ml.recommendation.ALS                                     |             |
| Estimator   | org.apache.spark.ml.feature.QuantileDiscretizer                            |             |
| Estimator   | org.apache.spark.ml.feature.UnivariateFeatureSelector                      |             |
| Estimator   | org.apache.spark.ml.feature.ChiSqSelector                                  |             |
| Estimator   | org.apache.spark.ml.feature.StandardScaler                                 |             |
| Estimator   | org.apache.spark.ml.feature.RFormula                                       |             |
| Estimator   | org.apache.spark.ml.classification.FMClassifier                            |             |
| Estimator   | org.apache.spark.ml.classification.NaiveBayes                              |             |
| Estimator   | org.apache.spark.ml.regression.AFTSurvivalRegression                       |             |
| Estimator   | org.apache.spark.ml.feature.RobustScaler                                   |             |
| Estimator   | org.apache.spark.ml.feature.PCA                                            |             |
| Estimator   | org.apache.spark.ml.feature.BucketedRandomProjectionLSH                    |             |


TBD Add Evaluators
