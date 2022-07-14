export class AuthEngine {
  constructor() {
    this._internalStorage = {};
    this.isLocalStorageEnabled = this._checkLocalStorageEnabled();
  }

  _checkLocalStorageEnabled() {
    let err;
    try {
      // Some browsers will throw an error here if localStorage is disabled.
      global.localStorage;
  
      // Safari, in Private Browsing Mode, looks like it supports localStorage but all calls to setItem
      // throw QuotaExceededError. We're going to detect this and avoid hard to debug edge cases.
      global.localStorage.setItem('__scLocalStorageTest', 1);
      global.localStorage.removeItem('__scLocalStorageTest');
    } catch (e) {
      err = e;
    }
    return !err;
  };

  saveToken(name, token, options) {
    if (this.isLocalStorageEnabled && global.localStorage) {
      global.localStorage.setItem(name, token);
    } else {
      this._internalStorage[name] = token;
    }
    return Promise.resolve(token);
  };

  removeToken(name) {
    let loadPromise = this.loadToken(name);
  
    if (this.isLocalStorageEnabled && global.localStorage) {
      global.localStorage.removeItem(name);
    } else {
      delete this._internalStorage[name];
    }
  
    return loadPromise;
  };

  loadToken(name) {
    let token;
  
    if (this.isLocalStorageEnabled && global.localStorage) {
      token = global.localStorage.getItem(name);
    } else {
      token = this._internalStorage[name] || null;
    }
  
    return Promise.resolve(token);
  };
}
