function showLog(obj, result) {
    var panel = obj.parent().removeClass('panel-default');
    if (result.ok)
        panel.addClass('panel-success');
    else
        panel.addClass('panel-danger');
    for (x in result.log) {
        var thetime = new Date(result.log[x].time * 1000);
        obj.append($('<div>').append($('<div class="log-time">').html(thetime.toLocaleString()), $('<div class="log-msg">').html(result.log[x].msg)));
    }
}
$(document).ready(function () {
    $('#frmSnapshot').submit(function () {
        var data = $(this).serializeArray();
        $('#snapshotlog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'snapshot'), data).done(function (result) {
            showLog($('#snapshotlog'), result);
        });
        return false;
    });
    $('#frmMigrate').submit(function () {
        var data = $(this).serializeArray();
        $('#migratelog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'migrate'), data).done(function (result) {
            showLog($('#migratelog'), result);
        });
        return false;
    });
    $('#frmSync').submit(function () {
        $('#synclog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'syncdata')).done(function (result) {
            showLog($('#synclog'), result);
        });
        return false;
    });
});