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
    $('#btnSnapshot').click(function () {
        var data = $(this).parent().serializeArray();
        $('#snapshotlog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'snapshot'), data).done(function (result) {
            showLog($('#snapshotlog'), result);
        }).fail(handleError);
        return false;
    });
    $('#btnMigrate').click(function () {
        var data = $(this).parent().serializeArray();
        $('#migratelog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'migrate'), data).done(function (result) {
            showLog($('#migratelog'), result);
        }).fail(handleError);
        return false;
    });
    $('#btnDataSync').click(function () {
        $('#synclog').empty().parent().addClass('panel-default').removeClass('panel-success').removeClass('panel-danger');
        $.post(hazaar.url('dbi', 'sync')).done(function (result) {
            showLog($('#synclog'), result);
        }).fail(handleError);
        return false;
    });
    $('button.btnFSCK').click(function () {
        let label = $('<i class="label">').html('Working...');
        $(this).parent().next(".status").html(label);
        $.post(hazaar.url('dbi', 'fsck', { fs: $(this).attr('data-fsname') })).done(function (result) {
            if (result.ok) label.addClass('success').html('Check Completed Successfully!');
            else label.addClass('danger').html('Unknown Error');
            label.removeClass('default');
        }).fail(handleError);
        return false;
    });
});