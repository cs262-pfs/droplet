import logging

from sklearn.ensemble import BaggingRegressor


class CostPredictors:
    def __init__(self):
        self.fxModels = dict()
        self.fxXs = dict()
        self.fxYs = dict()
        self.fitted = set()

    def add_function(self, function):
        if function in self.fxModels:
            assert 'Function was already added'

        self.fxModels[function] = BaggingRegressor()
        self.fxXs[function] = []
        self.fxYs[function] = []

    def has_predictor(self, fname):
        return fname in self.fxModels and fname in self.fitted

    def compute_cost(self, function, X):
        try:
            return self.fxModels[function].predict(X)
        except:
            logging.info("Model not fit, returning default prediction")
            return 1 
        
    def add_new_result(self, function, X, y):
        self.fxXs[function].append([X])
        self.fxYs[function].append(y)
        
    def update_model(self, fx):
        if fx not in self.fxModels:
            assert "Function {}, does not exist".format(fx)
        if not (self.fxXs[fx] and self.fxYs[fx]):
            assert "Function must have training data prior to fitting"

        self.fxModels[fx].fit(self.fxXs[fx], self.fxYs[fx])
        self.fitted.add(fx)

    def update_models(self, fxs):
        for fx in fxs:
            self.update_model(fx)

    def update_all_models(self):
        for fx in self.fxModels.keys():
            self.update_model(fx)
