import app = require("durandal/app");
import router = require("plugins/router");
import shell = require("viewmodels/shell");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import getCustomColumnsCommand = require('commands/getCustomColumnsCommand');
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import customColumnParams = require('models/customColumnParams');
import customColumns = require('models/customColumns');
import changeSubscription = require('models/changeSubscription');
import changesApi = require("common/changesApi");
import customFunctions = require("models/customFunctions");
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");

class documents extends viewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
    collectionToSelectName: string;
    currentCollectionPagedItems = ko.observable<pagedList>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentCustomFunctions = ko.observable<customFunctions>(customFunctions.empty());
    selectedDocumentIndices = ko.observableArray<number>();
    isSelectAll = ko.observable(false);
    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    contextName = ko.observable<string>('');
    currentCollection = ko.observable<collection>();
    showLoadingIndicator: KnockoutObservable<boolean> = ko.observable<boolean>(false);

    static gridSelector = "#documentsGrid";

    constructor() {
        super();
        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
        this.hasAnyDocumentsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);
    }

    activate(args) {
        super.activate(args);

        this.fetchCustomFunctions();

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        var db = this.activeDatabase();
        this.fetchCollections(db).done(results => this.collectionsLoaded(results, db));
    }

    attached() {
        // Initialize the context menu (using Bootstrap-ContextMenu library).
        // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
        (<any>$('.document-collections')).contextmenu({
            target: '#collections-context-menu'
        });
    }

    private fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db).execute();
    }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentResourceChangesApi().watchAllIndexes((e: indexChangeNotificationDto) => this.changesApiIndexUpdated(e)),
            shell.currentResourceChangesApi().watchAllDocs(() => this.changesApiDocumentsUpdated()),
            shell.currentResourceChangesApi().watchBulks(() => this.changesApiDocumentsUpdated())
        ];
    }

    private changesApiIndexUpdated(e: indexChangeNotificationDto) {
        if (e.Name === "Raven/DocumentsByEntityName") {
            var db = this.activeDatabase();
            this.fetchCollections(db).done(results => this.updateCollections(results, db));
        }
    }

    private changesApiDocumentsUpdated() {
        var db = this.activeDatabase();
        this.fetchCollections(db).done(results => this.updateCollections(results, db));
    }

    collectionsLoaded(collections: Array<collection>, db: database) {
        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = collection.createAllDocsCollection(db);

        // Create the "System Documents" pseudo collection.
        var systemDocumentsCollection = collection.createSystemDocsCollection(db);

        // All systems a-go. Load them into the UI and select the first one.
        var collectionsWithSysCollection = [systemDocumentsCollection].concat(collections);
        var allCollections = [this.allDocumentsCollection].concat(collectionsWithSysCollection);
        this.collections(allCollections);

        var collectionToSelect = allCollections.first(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection;
        collectionToSelect.activate();
    }

    fetchCustomFunctions() {
        var customFunctionsCommand = new getCustomFunctionsCommand(this.activeDatabase()).execute();
        customFunctionsCommand.done((cf: customFunctions) => {
            this.currentCustomFunctions(cf);
        });
    }

    //TODO: this binding has notification leak!
    selectedCollectionChanged(selected: collection) {
        if (selected) {
            this.isSelectAll(false);

            var customColumnsCommand = selected.isAllDocuments ?
                getCustomColumnsCommand.forAllDocuments(this.activeDatabase()) : getCustomColumnsCommand.forCollection(selected.name, this.activeDatabase());

            this.contextName(customColumnsCommand.docName);

            customColumnsCommand.execute().done((dto: customColumnsDto) => {
                if (dto) {
                    this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                    this.currentColumnsParams().customMode(true);
                } else {
                    // use default values!
                    this.currentColumnsParams().columns.removeAll();
                    this.currentColumnsParams().customMode(false);
                }

                var pagedList = selected.getDocuments();
                this.currentCollectionPagedItems(pagedList);
                this.currentCollection(selected);
            });
        }
    }

    deleteCollection() {
        var collection = this.selectedCollection();
        if (collection) {
            var viewModel = new deleteCollection(collection);
            viewModel.deletionTask.done(() => {
                this.collections.remove(collection);
                this.allDocumentsCollection.activate();
            });
            app.showDialog(viewModel);
        }
    }

    private updateCollections(receivedCollections: Array<collection>, db: database) {
        var deletedCollections = [];

        this.collections().forEach((col: collection) => {
            if (!receivedCollections.first((receivedCol: collection) => col.name == receivedCol.name) && col.name != 'System Documents' && col.name != 'All Documents') {
                deletedCollections.push(col);
            }
        });

        this.collections.removeAll(deletedCollections);

        receivedCollections.forEach((receivedCol: collection) => {
            var foundCollection = this.collections().first((col: collection) => col.name == receivedCol.name);
            if (!foundCollection) {
                this.collections.push(receivedCol);
            } else {
                foundCollection.documentCount(receivedCol.documentCount());
            }
        });

        //if the collection is deleted, go to the all documents collection
        var currentCollection: collection = this.collections().first(c => c.name === this.selectedCollection().name);
        if (!currentCollection || currentCollection.documentCount() == 0) {
            this.selectCollection(this.allDocumentsCollection);
        }
    }
    
    selectCollection(collection: collection) {
        collection.activate();
        
        var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
        router.navigate(documentsWithCollectionUrl, false);
    }

    selectColumns() {
        require(["viewmodels/selectColumns"], selectColumns => {

            // Fetch column widths from virtual table
            var virtualTable = this.getDocumentsGrid();
            var vtColumns = virtualTable.columns();
            this.currentColumnsParams().columns().forEach( (column: customColumnParams) => {
                for(var i=0; i < vtColumns.length; i++) {
                    if (column.binding() === vtColumns[i].binding) {
                        column.width(vtColumns[i].width()|0);
                        break;
                    }
                }
            });

            var selectColumnsViewModel = new selectColumns(this.currentColumnsParams().clone(), this.currentCustomFunctions(), this.contextName(), this.activeDatabase());
            app.showDialog(selectColumnsViewModel);
            selectColumnsViewModel.onExit().done((cols) => {
                this.currentColumnsParams(cols);

                var pagedList = this.currentCollection().getDocuments();
                this.currentCollectionPagedItems(pagedList);
            });
        });
    }

    newDocument() {
        router.navigate(appUrl.forNewDoc(this.activeDatabase()));
    }

    toggleSelectAll() {
        this.isSelectAll.toggle();

        var docsGrid = this.getDocumentsGrid();
        if (docsGrid && this.isSelectAll()) {
            docsGrid.selectAll();
        } else if (docsGrid && !this.isSelectAll()) {
            docsGrid.selectNone();
        }        
    }

    editSelectedDoc() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    deleteSelectedDocs() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.deleteSelectedItems();
        }
    }
    

    copySelectedDocs() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocs();
        }
    }

    copySelectedDocIds() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocIds();
        }
    }

    getDocumentsGrid(): virtualTable {
        var gridContents = $(documents.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = documents;
