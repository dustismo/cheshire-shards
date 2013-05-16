
// simple rolling log 
function Log(config) {
    this.config = $.extend({
        template : null,
        //max number of entries to display
        max : 50,
        container : "#log-container",
        entryId : "logentry"
    }, config);
    if (!this.config.template) {
        this.config.template = Handlebars.compile("<div id=\"{{id}}\" class=\"log-{{type}}\">{{message}}</div>");
    }
    this.id = 0
    //adds a new message
    this.message = function(type, message) {
        i = this.id++ 
        id = this.config.entryId+i
        $(this.config.container).append(this.config.template({id : id, type : type, message : message}))

        $('#'+this.config.entryId +(i-this.config.max)).remove();
        $(this.config.container).scrollTo('max')

    }
}