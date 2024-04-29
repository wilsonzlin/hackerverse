type Listener = () => void;

class Router {
  private readonly listeners = new Set<Listener>();

  constructor() {
    window.addEventListener("popstate", this._triggerListeners);
  }

  private _triggerListeners = () => {
    for (const l of this.listeners) {
      l();
    }
  };

  addListener(l: Listener) {
    this.listeners.add(l);
  }

  removeListener(l: Listener) {
    this.listeners.delete(l);
  }

  change(pathname: string) {
    history.pushState(undefined, "", pathname);
    this._triggerListeners();
  }
}

export const router = new Router();
