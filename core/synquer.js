class Synquer {

    constructor() {
        this.queue = [];
        this.isDraining = false;
    }

    wait() {
        return new Promise((resolve) => {
            const check = () => {
                if (this.queue.length) {
                    setImmediate(check);
                } else {
                    resolve();
                }
            }
            setImmediate(check);
        });
    }

    async drain() {
        this.isDraining = true;
        if (this.queue.length) {
            const job = this.queue.shift();
            await this.run(job);
        } else {
            this.isDraining = false; //no more itens to drain
        }
    }
    async run(job) {
        this.isRunning = true;
        try {
            const response = await job.action();
            if (typeof job.resolve === "function") {
                job.resolve(response);
            }
            return response;
        } catch (ex) {
            if (typeof job.reject === "function") {
                job.reject(ex);
            } else {
                throw ex;
            }
        } finally {
            this.isRunning = false;
            //drain start after run!
            this.drain();
        }
    }

    execute(action) {

        //if its not draining and not running just run!
        if (!this.isDraining && !this.isRunning) {
            return this.run({ action });
        }

        //if something its draining or running add to queue!
        return new Promise((resolve, reject) => {
            //add to queue!
            this.queue.push({ resolve, reject, action });
        });
    }
}

module.exports =  { Synquer };
module.exports["default"] = Synquer;