
/*
 * Copyright (C) Maxim Dounin
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


typedef struct {
    ngx_uint_t                         max_cached;
    ngx_uint_t                         requests;
    ngx_msec_t                         timeout;
    ngx_flag_t                         reject;

    ngx_queue_t                        cache;
    ngx_queue_t                        free;

    ngx_http_upstream_init_pt          original_init_upstream;
    ngx_http_upstream_init_peer_pt     original_init_peer;

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    struct {
        ngx_flag_t reject;
        ngx_msec_t timeout;
        ngx_queue_t queue;
        ngx_uint_t max;
        ngx_uint_t size;
    } kp;
#endif

} ngx_http_upstream_keepalive_srv_conf_t;


typedef struct {
    ngx_http_upstream_keepalive_srv_conf_t  *conf;

    ngx_queue_t                        queue;
    ngx_connection_t                  *connection;

    socklen_t                          socklen;
    ngx_sockaddr_t                     sockaddr;

} ngx_http_upstream_keepalive_cache_t;


typedef struct {
    ngx_http_upstream_keepalive_srv_conf_t  *conf;

    ngx_http_upstream_t               *upstream;

    void                              *data;

    ngx_event_get_peer_pt              original_get_peer;
    ngx_event_free_peer_pt             original_free_peer;

#if (NGX_HTTP_SSL)
    ngx_event_set_peer_session_pt      original_set_session;
    ngx_event_save_peer_session_pt     original_save_session;
#endif

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    ngx_http_request_t *request;
    ngx_event_t timeout;
    ngx_queue_t queue;
#endif

} ngx_http_upstream_keepalive_peer_data_t;


static ngx_int_t ngx_http_upstream_init_keepalive_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_keepalive_peer(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_free_keepalive_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);

static void ngx_http_upstream_keepalive_dummy_handler(ngx_event_t *ev);
static void ngx_http_upstream_keepalive_close_handler(ngx_event_t *ev);
static void ngx_http_upstream_keepalive_close(ngx_connection_t *c);

#if (NGX_HTTP_SSL)
static ngx_int_t ngx_http_upstream_keepalive_set_session(
    ngx_peer_connection_t *pc, void *data);
static void ngx_http_upstream_keepalive_save_session(ngx_peer_connection_t *pc,
    void *data);
#endif

static void *ngx_http_upstream_keepalive_create_conf(ngx_conf_t *cf);
static char *ngx_http_upstream_keepalive(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
static char *ngx_http_upstream_keepalive_queue_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_http_upstream_keepalive_srv_conf_t *kcf = conf;
    if (!kcf->max_cached) return "works only with \"keepalive\"";
    if (kcf->kp.max) return "duplicate";
    ngx_str_t *args = cf->args->elts;
    ngx_int_t n = ngx_atoi(args[1].data, args[1].len);
    if (n == NGX_ERROR) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"%V\" must be number", &cmd->name, &args[1]); return NGX_CONF_ERROR; }
    if (n <= 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"%V\" must be positive", &cmd->name, &args[1]); return NGX_CONF_ERROR; }
    kcf->kp.max = (ngx_uint_t)n;
    for (ngx_uint_t i = 2; i < cf->args->nelts; i++) {
        if (args[i].len > sizeof("overflow=") - 1 && !ngx_strncasecmp(args[i].data, (u_char *)"overflow=", sizeof("overflow=") - 1)) {
            args[i].len = args[i].len - (sizeof("overflow=") - 1);
            args[i].data = &args[i].data[sizeof("overflow=") - 1];
            static const ngx_conf_enum_t e[] = {
                { ngx_string("ignore"), 0 },
                { ngx_string("reject"), 1 },
                { ngx_null_string, 0 }
            };
            ngx_uint_t j;
            for (j = 0; e[j].name.len; j++) if (e[j].name.len == args[i].len && !ngx_strncasecmp(e[j].name.data, args[i].data, args[i].len)) { kcf->kp.reject = e[j].value; break; }
            if (!e[j].name.len) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"overflow\" value \"%V\" must be \"ignore\" or \"reject\"", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            continue;
        }
        if (args[i].len > sizeof("timeout=") - 1 && !ngx_strncasecmp(args[i].data, (u_char *)"timeout=", sizeof("timeout=") - 1)) {
            args[i].len = args[i].len - (sizeof("timeout=") - 1);
            args[i].data = &args[i].data[sizeof("timeout=") - 1];
            ngx_int_t n = ngx_parse_time(&args[i], 0);
            if (n == NGX_ERROR) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"timeout\" value \"%V\" must be time", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            if (n <= 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"timeout\" value \"%V\" must be positive", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            kcf->kp.timeout = (ngx_msec_t)n;
            continue;
        }
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: invalid additional parameter \"%V\"", &cmd->name, &args[i]);
        return NGX_CONF_ERROR;
    }
    return NGX_CONF_OK;
}
#endif


static ngx_command_t  ngx_http_upstream_keepalive_commands[] = {

    { ngx_string("keepalive"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE12|NGX_CONF_TAKE3|NGX_CONF_TAKE4,
      ngx_http_upstream_keepalive,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("keepalive_timeout"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_upstream_keepalive_srv_conf_t, timeout),
      NULL },

    { ngx_string("keepalive_requests"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_upstream_keepalive_srv_conf_t, requests),
      NULL },

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
  { .name = ngx_string("queue"),
    .type = NGX_HTTP_UPS_CONF|NGX_CONF_TAKE12|NGX_CONF_TAKE3,
    .set = ngx_http_upstream_keepalive_queue_conf,
    .conf = NGX_HTTP_SRV_CONF_OFFSET,
    .offset = 0,
    .post = NULL },
#endif

      ngx_null_command
};


static ngx_http_module_t  ngx_http_upstream_keepalive_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_http_upstream_keepalive_create_conf, /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};


ngx_module_t  ngx_http_upstream_keepalive_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_keepalive_module_ctx, /* module context */
    ngx_http_upstream_keepalive_commands,    /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_upstream_init_keepalive(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                               i;
    ngx_http_upstream_keepalive_srv_conf_t  *kcf;
    ngx_http_upstream_keepalive_cache_t     *cached;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
                   "init keepalive");

    kcf = ngx_http_conf_upstream_srv_conf(us,
                                          ngx_http_upstream_keepalive_module);

    ngx_conf_init_msec_value(kcf->timeout, 60000);
    ngx_conf_init_uint_value(kcf->requests, 100);

    if (kcf->original_init_upstream(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    kcf->original_init_peer = us->peer.init;

    us->peer.init = ngx_http_upstream_init_keepalive_peer;

    /* allocate cache items and add to free queue */

    cached = ngx_pcalloc(cf->pool,
                sizeof(ngx_http_upstream_keepalive_cache_t) * kcf->max_cached);
    if (cached == NULL) {
        return NGX_ERROR;
    }

    ngx_queue_init(&kcf->cache);
    ngx_queue_init(&kcf->free);

    for (i = 0; i < kcf->max_cached; i++) {
        ngx_queue_insert_head(&kcf->free, &cached[i].queue);
        cached[i].conf = kcf;
    }

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    ngx_queue_init(&kcf->kp.queue);
    if (!kcf->kp.max) return NGX_OK;
    ngx_conf_init_msec_value(kcf->kp.timeout, 60 * 1000);
#endif

    return NGX_OK;
}


#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
static void ngx_http_upstream_keepalive_peer_data_cleanup(void *data) {
    ngx_http_upstream_keepalive_peer_data_t *kp = data;
    ngx_http_request_t *r = kp->request;
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "%s", __func__);
    if (!ngx_queue_empty(&kp->queue)) {
        ngx_queue_remove(&kp->queue);
        ngx_queue_init(&kp->queue);
        ngx_http_upstream_keepalive_srv_conf_t *kcf = kp->conf;
        if (kcf->kp.size) kcf->kp.size--;
    }
    if (kp->timeout.timer_set) ngx_del_timer(&kp->timeout);
}
#endif


static ngx_int_t
ngx_http_upstream_init_keepalive_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_keepalive_peer_data_t  *kp;
    ngx_http_upstream_keepalive_srv_conf_t   *kcf;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "init keepalive peer");

    kcf = ngx_http_conf_upstream_srv_conf(us,
                                          ngx_http_upstream_keepalive_module);

    kp = ngx_palloc(r->pool, sizeof(ngx_http_upstream_keepalive_peer_data_t));
    if (kp == NULL) {
        return NGX_ERROR;
    }

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    kp->request = r;
    ngx_queue_init(&kp->queue);
    ngx_pool_cleanup_t *cln = ngx_pool_cleanup_add(r->pool, 0);
    if (!cln) { ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "!ngx_pool_cleanup_add"); return NGX_ERROR; }
    cln->handler = ngx_http_upstream_keepalive_peer_data_cleanup;
    cln->data = kp;
#endif

    if (kcf->original_init_peer(r, us) != NGX_OK) {
        return NGX_ERROR;
    }

    kp->conf = kcf;
    kp->upstream = r->upstream;
    kp->data = r->upstream->peer.data;
    kp->original_get_peer = r->upstream->peer.get;
    kp->original_free_peer = r->upstream->peer.free;

    r->upstream->peer.data = kp;
    r->upstream->peer.get = ngx_http_upstream_get_keepalive_peer;
    r->upstream->peer.free = ngx_http_upstream_free_keepalive_peer;

#if (NGX_HTTP_SSL)
    kp->original_set_session = r->upstream->peer.set_session;
    kp->original_save_session = r->upstream->peer.save_session;
    r->upstream->peer.set_session = ngx_http_upstream_keepalive_set_session;
    r->upstream->peer.save_session = ngx_http_upstream_keepalive_save_session;
#endif

    return NGX_OK;
}


#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
static void ngx_http_upstream_keepalive_peer_data_timeout_handler(ngx_event_t *ev) {
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, ev->log, 0, "write = %s", ev->write ? "true" : "false");
    ngx_http_upstream_keepalive_peer_data_t *kp = ev->data;
    ngx_http_request_t *r = kp->request;
    if (!ngx_queue_empty(&kp->queue)) ngx_queue_remove(&kp->queue);
    ngx_queue_init(&kp->queue);
    ngx_http_upstream_keepalive_srv_conf_t *kcf = kp->conf;
    if (kcf->kp.size) kcf->kp.size--;
    if (!r->connection || r->connection->error) return;
    ngx_http_upstream_next(r, r->upstream, NGX_HTTP_UPSTREAM_FT_TIMEOUT);
}
#endif


static ngx_int_t
ngx_http_upstream_get_keepalive_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_keepalive_peer_data_t  *kp = data;
    ngx_http_upstream_keepalive_cache_t      *item;

    ngx_int_t          rc;
    ngx_queue_t       *q, *cache;
    ngx_connection_t  *c;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "get keepalive peer");

    /* ask balancer */

    rc = kp->original_get_peer(pc, kp->data);

    if (rc != NGX_OK) {
        return rc;
    }

    /* search cache for suitable connection */

    cache = &kp->conf->cache;

    for (q = ngx_queue_head(cache);
         q != ngx_queue_sentinel(cache);
         q = ngx_queue_next(q))
    {
        item = ngx_queue_data(q, ngx_http_upstream_keepalive_cache_t, queue);
        c = item->connection;

        if (ngx_memn2cmp((u_char *) &item->sockaddr, (u_char *) pc->sockaddr,
                         item->socklen, pc->socklen)
            == 0)
        {
            ngx_queue_remove(q);
            ngx_queue_insert_head(&kp->conf->free, q);

            goto found;
        }
    }

    ngx_http_upstream_keepalive_srv_conf_t *kcf = kp->conf;
    if (!ngx_queue_empty(&kcf->free)) {

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    } else if (kcf->kp.max) {
        if (kcf->kp.size < kcf->kp.max) {
            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "kp = %p", kp);
            ngx_queue_insert_tail(&kcf->kp.queue, &kp->queue);
            kcf->kp.size++;
            kp->timeout.handler = ngx_http_upstream_keepalive_peer_data_timeout_handler;
            kp->timeout.log = pc->log;
            kp->timeout.data = kp;
            ngx_add_timer(&kp->timeout, kcf->kp.timeout);
            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "kp.size = %i", kcf->kp.size);
            return NGX_YIELD;
        } else if (kcf->kp.reject) {
            ngx_log_error(NGX_LOG_WARN, pc->log, 0, "kp.size = %i", kcf->kp.size);
            return NGX_BUSY;
        }
#endif

    } else if (kcf->reject) return NGX_BUSY;

    return NGX_OK;

found:

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "get keepalive peer: using connection %p", c);

    c->idle = 0;
    c->sent = 0;
    c->data = NULL;
    c->log = pc->log;
    c->read->log = pc->log;
    c->write->log = pc->log;
    c->pool->log = pc->log;

    if (c->read->timer_set) {
        ngx_del_timer(c->read);
    }

    pc->connection = c;
    pc->cached = 1;

    return NGX_DONE;
}


static void
ngx_http_upstream_free_keepalive_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_keepalive_peer_data_t  *kp = data;
    ngx_http_upstream_keepalive_cache_t      *item;

    ngx_queue_t          *q;
    ngx_connection_t     *c;
    ngx_http_upstream_t  *u;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "free keepalive peer");

    /* cache valid connections */

    u = kp->upstream;
    c = pc->connection;

    if (state & NGX_PEER_FAILED
        || c == NULL
        || c->read->eof
        || c->read->error
        || c->read->timedout
        || c->write->error
        || c->write->timedout)
    {
        goto invalid;
    }

    if (c->requests >= kp->conf->requests) {
        goto invalid;
    }

    if (!u->keepalive) {
        goto invalid;
    }

    if (!u->request_body_sent) {
        goto invalid;
    }

    if (ngx_terminate || ngx_exiting) {
        goto invalid;
    }

    if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        goto invalid;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "free keepalive peer: saving connection %p", c);

    if (ngx_queue_empty(&kp->conf->free)) {

        q = ngx_queue_last(&kp->conf->cache);
        ngx_queue_remove(q);

        item = ngx_queue_data(q, ngx_http_upstream_keepalive_cache_t, queue);

        ngx_http_upstream_keepalive_close(item->connection);

    } else {
        q = ngx_queue_head(&kp->conf->free);
        ngx_queue_remove(q);

        item = ngx_queue_data(q, ngx_http_upstream_keepalive_cache_t, queue);
    }

    ngx_queue_insert_head(&kp->conf->cache, q);

    item->connection = c;

    pc->connection = NULL;

    c->read->delayed = 0;
    ngx_add_timer(c->read, kp->conf->timeout);

    if (c->write->timer_set) {
        ngx_del_timer(c->write);
    }

    c->write->handler = ngx_http_upstream_keepalive_dummy_handler;
    c->read->handler = ngx_http_upstream_keepalive_close_handler;

    c->data = item;
    c->idle = 1;
    c->log = ngx_cycle->log;
    c->read->log = ngx_cycle->log;
    c->write->log = ngx_cycle->log;
    c->pool->log = ngx_cycle->log;

    item->socklen = pc->socklen;
    ngx_memcpy(&item->sockaddr, pc->sockaddr, pc->socklen);

    if (c->read->ready) {
        ngx_http_upstream_keepalive_close_handler(c->read);
    }

invalid:

    kp->original_free_peer(pc, kp->data, state);

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    ngx_http_upstream_keepalive_srv_conf_t *kcf = kp->conf;
    while (!ngx_queue_empty(&kcf->kp.queue)) {
        ngx_queue_t *queue = ngx_queue_head(&kcf->kp.queue);
        ngx_queue_remove(queue);
        ngx_queue_init(queue);
        ngx_http_upstream_keepalive_peer_data_t *kp = ngx_queue_data(queue, ngx_http_upstream_keepalive_peer_data_t, queue);
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "kp = %p", kp);
        if (kcf->kp.size) kcf->kp.size--;
        if (kp->timeout.timer_set) ngx_del_timer(&kp->timeout);
        ngx_http_request_t *r = kp->request;
        if (!r->connection || r->connection->error) continue;
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "kp = %p", kp);
        ngx_http_upstream_connect(r, r->upstream);
        break;
    }
#endif

}


static void
ngx_http_upstream_keepalive_dummy_handler(ngx_event_t *ev)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ev->log, 0,
                   "keepalive dummy handler");
}


static void
ngx_http_upstream_keepalive_close_handler(ngx_event_t *ev)
{
    ngx_http_upstream_keepalive_srv_conf_t  *conf;
    ngx_http_upstream_keepalive_cache_t     *item;

    int                n;
    char               buf[1];
    ngx_connection_t  *c;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ev->log, 0,
                   "keepalive close handler");

    c = ev->data;

    if (c->close || c->read->timedout) {
        goto close;
    }

    n = recv(c->fd, buf, 1, MSG_PEEK);

    if (n == -1 && ngx_socket_errno == NGX_EAGAIN) {
        ev->ready = 0;

        if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            goto close;
        }

        return;
    }

close:

    item = c->data;
    conf = item->conf;

    ngx_http_upstream_keepalive_close(c);

    ngx_queue_remove(&item->queue);
    ngx_queue_insert_head(&conf->free, &item->queue);
}


static void
ngx_http_upstream_keepalive_close(ngx_connection_t *c)
{

#if (NGX_HTTP_SSL)

    if (c->ssl) {
        c->ssl->no_wait_shutdown = 1;
        c->ssl->no_send_shutdown = 1;

        if (ngx_ssl_shutdown(c) == NGX_AGAIN) {
            c->ssl->handler = ngx_http_upstream_keepalive_close;
            return;
        }
    }

#endif

    ngx_destroy_pool(c->pool);
    ngx_close_connection(c);
}


#if (NGX_HTTP_SSL)

static ngx_int_t
ngx_http_upstream_keepalive_set_session(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_keepalive_peer_data_t  *kp = data;

    return kp->original_set_session(pc, kp->data);
}


static void
ngx_http_upstream_keepalive_save_session(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_keepalive_peer_data_t  *kp = data;

    kp->original_save_session(pc, kp->data);
    return;
}

#endif


static void *
ngx_http_upstream_keepalive_create_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_keepalive_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool,
                       sizeof(ngx_http_upstream_keepalive_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     conf->original_init_upstream = NULL;
     *     conf->original_init_peer = NULL;
     *     conf->max_cached = 0;
     */

    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->requests = NGX_CONF_UNSET_UINT;

#if (T_NGX_HTTP_DYNAMIC_RESOLVE)
    conf->kp.timeout = NGX_CONF_UNSET_MSEC;
#endif

    return conf;
}


static char *
ngx_http_upstream_keepalive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t            *uscf;
    ngx_http_upstream_keepalive_srv_conf_t  *kcf = conf;

    ngx_int_t    n;
    ngx_str_t   *value;

    if (kcf->max_cached) {
        return "is duplicate";
    }

    /* read options */

    value = cf->args->elts;

    n = ngx_atoi(value[1].data, value[1].len);

    if (n == NGX_ERROR || n == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid value \"%V\" in \"%V\" directive",
                           &value[1], &cmd->name);
        return NGX_CONF_ERROR;
    }

    kcf->max_cached = n;

    ngx_str_t *args = cf->args->elts;
    for (ngx_uint_t i = 2; i < cf->args->nelts; i++) {
        if (args[i].len > sizeof("overflow=") - 1 && !ngx_strncasecmp(args[i].data, (u_char *)"overflow=", sizeof("overflow=") - 1)) {
            args[i].len = args[i].len - (sizeof("overflow=") - 1);
            args[i].data = &args[i].data[sizeof("overflow=") - 1];
            static const ngx_conf_enum_t e[] = {
                { ngx_string("ignore"), 0 },
                { ngx_string("reject"), 1 },
                { ngx_null_string, 0 }
            };
            ngx_uint_t j;
            for (j = 0; e[j].name.len; j++) if (e[j].name.len == args[i].len && !ngx_strncasecmp(e[j].name.data, args[i].data, args[i].len)) { kcf->reject = e[j].value; break; }
            if (!e[j].name.len) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"overflow\" value \"%V\" must be \"ignore\" or \"reject\"", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            continue;
        }
        if (args[i].len > sizeof("timeout=") - 1 && !ngx_strncasecmp(args[i].data, (u_char *)"timeout=", sizeof("timeout=") - 1)) {
            args[i].len = args[i].len - (sizeof("timeout=") - 1);
            args[i].data = &args[i].data[sizeof("timeout=") - 1];
            ngx_int_t n = ngx_parse_time(&args[i], 0);
            if (n == NGX_ERROR) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"timeout\" value \"%V\" must be time", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            if (n <= 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"timeout\" value \"%V\" must be positive", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            kcf->timeout = (ngx_msec_t)n;
            continue;
        }
        if (args[i].len > sizeof("requests=") - 1 && !ngx_strncasecmp(args[i].data, (u_char *)"requests=", sizeof("requests=") - 1)) {
            args[i].len = args[i].len - (sizeof("requests=") - 1);
            args[i].data = &args[i].data[sizeof("requests=") - 1];
            ngx_int_t n = ngx_atoi(args[i].data, args[i].len);
            if (n == NGX_ERROR) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"requests\" value \"%V\" must be number", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            if (n <= 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: \"requests\" value \"%V\" must be positive", &cmd->name, &args[i]); return NGX_CONF_ERROR; }
            kcf->requests = (ngx_uint_t)n;
            continue;
        }
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "\"%V\" directive error: invalid additional parameter \"%V\"", &cmd->name, &args[i]);
        return NGX_CONF_ERROR;
    }

    /* init upstream handler */

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    kcf->original_init_upstream = uscf->peer.init_upstream
                                  ? uscf->peer.init_upstream
                                  : ngx_http_upstream_init_round_robin;

    uscf->peer.init_upstream = ngx_http_upstream_init_keepalive;

    return NGX_CONF_OK;
}
