import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class bulkInsertDetails extends abstractOperationDetails {

    progress: KnockoutObservable<Raven.Client.Documents.Operations.BulkInsertProgress>;
    result: KnockoutObservable<Raven.Client.Documents.Operations.BulkOperationResult>;
    processingSpeed: KnockoutComputed<string>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();

        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Client.Documents.Operations.BulkInsertProgress;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Client.Documents.Operations.BulkOperationResult : null;
        });

        this.processingSpeed = ko.pureComputed(() => {
            const progress = this.progress();
            const processingSpeed = this.calculateProcessingSpeed(progress.Processed);
            if (processingSpeed === 0) {
                return "N/A";
            }

            return `${processingSpeed.toLocaleString()} docs / sec`;
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "BulkInsert";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new bulkInsertDetails(op, center));
    }

}

export = bulkInsertDetails;
