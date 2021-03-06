
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


typedef struct ngx_http_header_val_s  ngx_http_header_val_t;

typedef ngx_int_t (*ngx_http_set_header_pt)(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);


typedef struct {
    ngx_str_t                  name;
    ngx_uint_t                 offset;
    ngx_http_set_header_pt     handler;
} ngx_http_set_header_t;


struct ngx_http_header_val_s {
    ngx_http_complex_value_t   value;
    ngx_str_t                  key;
    ngx_http_set_header_pt     handler;
    ngx_uint_t                 offset;
    ngx_uint_t                 always;  /* unsigned  always:1 */
};


typedef enum {
    NGX_HTTP_EXPIRES_OFF,
    NGX_HTTP_EXPIRES_EPOCH,
    NGX_HTTP_EXPIRES_MAX,
    NGX_HTTP_EXPIRES_ACCESS,
    NGX_HTTP_EXPIRES_MODIFIED,
    NGX_HTTP_EXPIRES_DAILY,
    NGX_HTTP_EXPIRES_UNSET
} ngx_http_expires_t;


typedef struct {
    ngx_http_expires_t         expires;
    time_t                     expires_time;
    ngx_http_complex_value_t  *expires_value;
    ngx_array_t               *headers;
    ngx_array_t               *trailers;
    ngx_array_t               *input_headers;
    ngx_flag_t                 subrequest;
} ngx_http_headers_conf_t;

typedef struct {
    ngx_int_t                  requires_handler;
} ngx_http_headers_main_conf_t;


static ngx_int_t ngx_http_set_expires(ngx_http_request_t *r,
    ngx_http_headers_conf_t *conf);
static ngx_int_t ngx_http_parse_expires(ngx_str_t *value,
    ngx_http_expires_t *expires, time_t *expires_time, char **err);
static ngx_int_t ngx_http_add_multi_header_lines(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_add_multi_input_header_lines(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_add_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_add_input_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_last_modified(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_accept_ranges(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_content_length(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_content_type_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_response_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_headers_validate_host(ngx_str_t *host,
    ngx_pool_t *pool, ngx_uint_t alloc);
static ngx_int_t ngx_http_set_host_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_connection_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_user_agent_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_content_length_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);
static ngx_int_t ngx_http_set_request_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value);

static ngx_int_t ngx_http_headers_handler(ngx_http_request_t *r);

static void *ngx_http_headers_create_conf(ngx_conf_t *cf);
static char *ngx_http_headers_merge_conf(ngx_conf_t *cf,
    void *parent, void *child);
static ngx_int_t ngx_http_headers_filter_init(ngx_conf_t *cf);
static void *ngx_http_headers_filter_main(ngx_conf_t *cf);
static char *ngx_http_headers_expires(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_headers_add(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_headers_add_input(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_http_set_header_t  ngx_http_set_headers[] = {

    { ngx_string("Link"),
                 offsetof(ngx_http_headers_out_t, link),
                 ngx_http_add_multi_header_lines },

    { ngx_string("Server"),
                 offsetof(ngx_http_headers_out_t, server),
                 ngx_http_set_response_header },

    { ngx_string("Date"),
                 offsetof(ngx_http_headers_out_t, date),
                 ngx_http_set_response_header },

    { ngx_string("Content-Encoding"),
                 offsetof(ngx_http_headers_out_t, content_encoding),
                 ngx_http_set_response_header },

    { ngx_string("Location"),
                 offsetof(ngx_http_headers_out_t, location),
                 ngx_http_set_response_header },

    { ngx_string("Refresh"),
                 offsetof(ngx_http_headers_out_t, refresh),
                 ngx_http_set_response_header },

    { ngx_string("Last-Modified"),
                 offsetof(ngx_http_headers_out_t, last_modified),
                 ngx_http_set_last_modified },

    { ngx_string("Content-Range"),
                 offsetof(ngx_http_headers_out_t, content_range),
                 ngx_http_set_response_header },

    { ngx_string("Accept-Ranges"),
                 offsetof(ngx_http_headers_out_t, accept_ranges),
                 ngx_http_set_accept_ranges },

    { ngx_string("WWW-Authenticate"),
                 offsetof(ngx_http_headers_out_t, www_authenticate),
                 ngx_http_set_response_header },

    { ngx_string("Expires"),
                 offsetof(ngx_http_headers_out_t, expires),
                 ngx_http_set_response_header },

    { ngx_string("ETag"),
                 offsetof(ngx_http_headers_out_t, etag),
                 ngx_http_set_response_header },

    { ngx_string("Content-Length"),
                 offsetof(ngx_http_headers_out_t, content_length),
                 ngx_http_set_content_length },

    { ngx_string("Content-Type"),
                 0,
                 ngx_http_set_content_type_header },

    { ngx_string("Cache-Control"),
                 offsetof(ngx_http_headers_out_t, cache_control),
                 ngx_http_add_multi_header_lines },

    { ngx_null_string, 0, NULL }
};


static ngx_http_set_header_t  ngx_http_set_input_headers[] = {

    { ngx_string("Host"),
                 offsetof(ngx_http_headers_in_t, host),
                 ngx_http_set_host_header },

    { ngx_string("Connection"),
                 offsetof(ngx_http_headers_in_t, connection),
                 ngx_http_set_connection_header },

    { ngx_string("If-Modified-Since"),
                 offsetof(ngx_http_headers_in_t, if_modified_since),
                 ngx_http_set_request_header },

    { ngx_string("If-Unmodified-Since"),
                 offsetof(ngx_http_headers_in_t, if_unmodified_since),
                 ngx_http_set_request_header },

    { ngx_string("If-Match"),
                 offsetof(ngx_http_headers_in_t, if_match),
                 ngx_http_set_request_header },

    { ngx_string("If-None-Match"),
                 offsetof(ngx_http_headers_in_t, if_none_match),
                 ngx_http_set_request_header },

    { ngx_string("User-Agent"),
                 offsetof(ngx_http_headers_in_t, user_agent),
                 ngx_http_set_user_agent_header },

    { ngx_string("Referer"),
                 offsetof(ngx_http_headers_in_t, referer),
                 ngx_http_set_request_header },

    { ngx_string("Content-Length"),
                 offsetof(ngx_http_headers_in_t, content_length),
                 ngx_http_set_content_length_header },

    { ngx_string("Content-Range"),
                 offsetof(ngx_http_headers_in_t, content_range),
                 ngx_http_set_request_header },

    { ngx_string("Content-Type"),
                 offsetof(ngx_http_headers_in_t, content_type),
                 ngx_http_set_request_header },

    { ngx_string("Range"),
                 offsetof(ngx_http_headers_in_t, range),
                 ngx_http_set_request_header },

    { ngx_string("If-Range"),
                 offsetof(ngx_http_headers_in_t, if_range),
                 ngx_http_set_request_header },

    { ngx_string("Transfer-Encoding"),
                 offsetof(ngx_http_headers_in_t, transfer_encoding),
                 ngx_http_set_request_header },

    { ngx_string("TE"),
                 offsetof(ngx_http_headers_in_t, te),
                 ngx_http_set_request_header },

    { ngx_string("Expect"),
                 offsetof(ngx_http_headers_in_t, expect),
                 ngx_http_set_request_header },

    { ngx_string("Upgrade"),
                 offsetof(ngx_http_headers_in_t, upgrade),
                 ngx_http_set_request_header },

#if (NGX_HTTP_GZIP)
    { ngx_string("Accept-Encoding"),
                 offsetof(ngx_http_headers_in_t, accept_encoding),
                 ngx_http_set_request_header },

    { ngx_string("Via"), offsetof(ngx_http_headers_in_t, via),
                 ngx_http_set_request_header },
#endif

    { ngx_string("Authorization"),
                 offsetof(ngx_http_headers_in_t, authorization),
                 ngx_http_set_request_header },

    { ngx_string("Keep-Alive"),
                 offsetof(ngx_http_headers_in_t, keep_alive),
                 ngx_http_set_request_header },

#if (NGX_HTTP_X_FORWARDED_FOR)
    { ngx_string("X-Forwarded-For"),
                 offsetof(ngx_http_headers_in_t, x_forwarded_for),
                 ngx_http_add_multi_input_header_lines },

#endif

#if (NGX_HTTP_REALIP)
    { ngx_string("X-Real-IP"),
                 offsetof(ngx_http_headers_in_t, x_real_ip),
                 ngx_http_set_request_header },
#endif

#if (NGX_HTTP_HEADERS)
    { ngx_string("Accept"), offsetof(ngx_http_headers_in_t, accept),
                 ngx_http_set_request_header },

    { ngx_string("Accept-Language"),
                 offsetof(ngx_http_headers_in_t, accept_language),
                 ngx_http_set_request_header },
#endif

#if (NGX_HTTP_DAV)
    { ngx_string("Depth"), offsetof(ngx_http_headers_in_t, depth),
                 ngx_http_set_request_header },

    { ngx_string("Destination"), offsetof(ngx_http_headers_in_t, destination),
                 ngx_http_set_request_header },

    { ngx_string("Overwrite"), offsetof(ngx_http_headers_in_t, overwrite),
                 ngx_http_set_request_header },

    { ngx_string("Date"), offsetof(ngx_http_headers_in_t, date),
                 ngx_http_set_request_header },
#endif

    { ngx_string("Cookie"),
                 offsetof(ngx_http_headers_in_t, cookies),
                 ngx_http_add_multi_input_header_lines },

    { ngx_null_string, 0, NULL }
};


static ngx_command_t  ngx_http_headers_filter_commands[] = {

    { ngx_string("expires"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
                        |NGX_CONF_TAKE12,
      ngx_http_headers_expires,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("add_header"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
                        |NGX_CONF_TAKE23,
      ngx_http_headers_add,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_headers_conf_t, headers),
      NULL },

    { ngx_string("add_input_header"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
                        |NGX_CONF_TAKE2,
      ngx_http_headers_add_input,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_headers_conf_t, input_headers),
      NULL },

    { ngx_string("add_trailer"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
                        |NGX_CONF_TAKE23,
      ngx_http_headers_add,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_headers_conf_t, trailers),
      NULL },

    { ngx_string("add_header_subrequest"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
                        |NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_headers_conf_t, subrequest),
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_headers_filter_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_headers_filter_init,          /* postconfiguration */

    ngx_http_headers_filter_main,          /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_headers_create_conf,          /* create location configuration */
    ngx_http_headers_merge_conf            /* merge location configuration */
};


ngx_module_t  ngx_http_headers_filter_module = {
    NGX_MODULE_V1,
    &ngx_http_headers_filter_module_ctx,   /* module context */
    ngx_http_headers_filter_commands,      /* module directives */
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


static ngx_http_output_header_filter_pt  ngx_http_next_header_filter;
static ngx_http_output_body_filter_pt    ngx_http_next_body_filter;


static ngx_int_t
ngx_http_headers_filter(ngx_http_request_t *r)
{
    ngx_str_t                 value;
    ngx_uint_t                i, safe_status;
    ngx_http_header_val_t    *h;
    ngx_http_headers_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_headers_filter_module);

    if (!conf->subrequest && r != r->main) {
        return ngx_http_next_header_filter(r);
    }

    if (conf->expires == NGX_HTTP_EXPIRES_OFF
        && conf->headers == NGX_CONF_UNSET_PTR
        && conf->trailers == NGX_CONF_UNSET_PTR)
    {
        return ngx_http_next_header_filter(r);
    }

    switch (r->headers_out.status) {

    case NGX_HTTP_OK:
    case NGX_HTTP_CREATED:
    case NGX_HTTP_NO_CONTENT:
    case NGX_HTTP_PARTIAL_CONTENT:
    case NGX_HTTP_MOVED_PERMANENTLY:
    case NGX_HTTP_MOVED_TEMPORARILY:
    case NGX_HTTP_SEE_OTHER:
    case NGX_HTTP_NOT_MODIFIED:
    case NGX_HTTP_TEMPORARY_REDIRECT:
    case NGX_HTTP_PERMANENT_REDIRECT:
        safe_status = 1;
        break;

    default:
        safe_status = 0;
        break;
    }

    if (conf->expires != NGX_HTTP_EXPIRES_OFF && safe_status) {
        if (ngx_http_set_expires(r, conf) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    if (conf->headers != NGX_CONF_UNSET_PTR) {
        h = conf->headers->elts;
        for (i = 0; i < conf->headers->nelts; i++) {

            if (!safe_status && !h[i].always) {
                continue;
            }

            if (ngx_http_complex_value(r, &h[i].value, &value) != NGX_OK) {
                return NGX_ERROR;
            }

            if (h[i].handler(conf->subrequest ? r->main : r, &h[i], &value) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    if (conf->trailers != NGX_CONF_UNSET_PTR) {
        h = conf->trailers->elts;
        for (i = 0; i < conf->trailers->nelts; i++) {

            if (!safe_status && !h[i].always) {
                continue;
            }

            r->expect_trailers = 1;
            break;
        }
    }

    return ngx_http_next_header_filter(r);
}


static ngx_int_t
ngx_http_trailers_filter(ngx_http_request_t *r, ngx_chain_t *in)
{
    ngx_str_t                 value;
    ngx_uint_t                i, safe_status;
    ngx_chain_t              *cl;
    ngx_table_elt_t          *t;
    ngx_http_header_val_t    *h;
    ngx_http_headers_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_headers_filter_module);

    if (in == NULL
        || conf->trailers == NGX_CONF_UNSET_PTR
        || !r->expect_trailers
        || r->header_only)
    {
        return ngx_http_next_body_filter(r, in);
    }

    for (cl = in; cl; cl = cl->next) {
        if (cl->buf->last_buf) {
            break;
        }
    }

    if (cl == NULL) {
        return ngx_http_next_body_filter(r, in);
    }

    switch (r->headers_out.status) {

    case NGX_HTTP_OK:
    case NGX_HTTP_CREATED:
    case NGX_HTTP_NO_CONTENT:
    case NGX_HTTP_PARTIAL_CONTENT:
    case NGX_HTTP_MOVED_PERMANENTLY:
    case NGX_HTTP_MOVED_TEMPORARILY:
    case NGX_HTTP_SEE_OTHER:
    case NGX_HTTP_NOT_MODIFIED:
    case NGX_HTTP_TEMPORARY_REDIRECT:
    case NGX_HTTP_PERMANENT_REDIRECT:
        safe_status = 1;
        break;

    default:
        safe_status = 0;
        break;
    }

    h = conf->trailers->elts;
    for (i = 0; i < conf->trailers->nelts; i++) {

        if (!safe_status && !h[i].always) {
            continue;
        }

        if (ngx_http_complex_value(r, &h[i].value, &value) != NGX_OK) {
            return NGX_ERROR;
        }

        if (value.len) {
            t = ngx_list_push(&r->headers_out.trailers);
            if (t == NULL) {
                return NGX_ERROR;
            }

            t->key = h[i].key;
            t->value = value;
            t->hash = 1;
        }
    }

    return ngx_http_next_body_filter(r, in);
}


static ngx_int_t
ngx_http_set_expires(ngx_http_request_t *r, ngx_http_headers_conf_t *conf)
{
    char                *err;
    size_t               len;
    time_t               now, expires_time, max_age;
    ngx_str_t            value;
    ngx_int_t            rc;
    ngx_uint_t           i;
    ngx_table_elt_t     *e, *cc, **ccp;
    ngx_http_expires_t   expires;

    expires = conf->expires;
    expires_time = conf->expires_time;

    if (conf->expires_value != NULL) {

        if (ngx_http_complex_value(r, conf->expires_value, &value) != NGX_OK) {
            return NGX_ERROR;
        }

        rc = ngx_http_parse_expires(&value, &expires, &expires_time, &err);

        if (rc != NGX_OK) {
            return NGX_OK;
        }

        if (expires == NGX_HTTP_EXPIRES_OFF) {
            return NGX_OK;
        }
    }

    e = r->headers_out.expires;

    if (e == NULL) {

        e = ngx_list_push(&r->headers_out.headers);
        if (e == NULL) {
            return NGX_ERROR;
        }

        r->headers_out.expires = e;

        e->hash = 1;
        ngx_str_set(&e->key, "Expires");
    }

    len = sizeof("Mon, 28 Sep 1970 06:00:00 GMT");
    e->value.len = len - 1;

    ccp = r->headers_out.cache_control.elts;

    if (ccp == NULL) {

        if (ngx_array_init(&r->headers_out.cache_control, r->pool,
                           1, sizeof(ngx_table_elt_t *))
            != NGX_OK)
        {
            return NGX_ERROR;
        }

        cc = ngx_list_push(&r->headers_out.headers);
        if (cc == NULL) {
            return NGX_ERROR;
        }

        cc->hash = 1;
        ngx_str_set(&cc->key, "Cache-Control");

        ccp = ngx_array_push(&r->headers_out.cache_control);
        if (ccp == NULL) {
            return NGX_ERROR;
        }

        *ccp = cc;

    } else {
        for (i = 1; i < r->headers_out.cache_control.nelts; i++) {
            ccp[i]->hash = 0;
        }

        cc = ccp[0];
    }

    if (expires == NGX_HTTP_EXPIRES_EPOCH) {
        e->value.data = (u_char *) "Thu, 01 Jan 1970 00:00:01 GMT";
        ngx_str_set(&cc->value, "no-cache");
        return NGX_OK;
    }

    if (expires == NGX_HTTP_EXPIRES_MAX) {
        e->value.data = (u_char *) "Thu, 31 Dec 2037 23:55:55 GMT";
        /* 10 years */
        ngx_str_set(&cc->value, "max-age=315360000");
        return NGX_OK;
    }

    e->value.data = ngx_pnalloc(r->pool, len);
    if (e->value.data == NULL) {
        return NGX_ERROR;
    }

    if (expires_time == 0 && expires != NGX_HTTP_EXPIRES_DAILY) {
        ngx_memcpy(e->value.data, ngx_cached_http_time.data,
                   ngx_cached_http_time.len + 1);
        ngx_str_set(&cc->value, "max-age=0");
        return NGX_OK;
    }

    now = ngx_time();

    if (expires == NGX_HTTP_EXPIRES_DAILY) {
        expires_time = ngx_next_time(expires_time);
        max_age = expires_time - now;

    } else if (expires == NGX_HTTP_EXPIRES_ACCESS
               || r->headers_out.last_modified_time == -1)
    {
        max_age = expires_time;
        expires_time += now;

    } else {
        expires_time += r->headers_out.last_modified_time;
        max_age = expires_time - now;
    }

    ngx_http_time(e->value.data, expires_time);

    if (conf->expires_time < 0 || max_age < 0) {
        ngx_str_set(&cc->value, "no-cache");
        return NGX_OK;
    }

    cc->value.data = ngx_pnalloc(r->pool,
                                 sizeof("max-age=") + NGX_TIME_T_LEN + 1);
    if (cc->value.data == NULL) {
        return NGX_ERROR;
    }

    cc->value.len = ngx_sprintf(cc->value.data, "max-age=%T", max_age)
                    - cc->value.data;

    return NGX_OK;
}


static ngx_int_t
ngx_http_parse_expires(ngx_str_t *value, ngx_http_expires_t *expires,
    time_t *expires_time, char **err)
{
    ngx_uint_t  minus;

    if (*expires != NGX_HTTP_EXPIRES_MODIFIED) {

        if (value->len == 5 && ngx_strncmp(value->data, "epoch", 5) == 0) {
            *expires = NGX_HTTP_EXPIRES_EPOCH;
            return NGX_OK;
        }

        if (value->len == 3 && ngx_strncmp(value->data, "max", 3) == 0) {
            *expires = NGX_HTTP_EXPIRES_MAX;
            return NGX_OK;
        }

        if (value->len == 3 && ngx_strncmp(value->data, "off", 3) == 0) {
            *expires = NGX_HTTP_EXPIRES_OFF;
            return NGX_OK;
        }
    }

    if (value->len && value->data[0] == '@') {
        value->data++;
        value->len--;
        minus = 0;

        if (*expires == NGX_HTTP_EXPIRES_MODIFIED) {
            *err = "daily time cannot be used with \"modified\" parameter";
            return NGX_ERROR;
        }

        *expires = NGX_HTTP_EXPIRES_DAILY;

    } else if (value->len && value->data[0] == '+') {
        value->data++;
        value->len--;
        minus = 0;

    } else if (value->len && value->data[0] == '-') {
        value->data++;
        value->len--;
        minus = 1;

    } else {
        minus = 0;
    }

    *expires_time = ngx_parse_time(value, 1);

    if (*expires_time == (time_t) NGX_ERROR) {
        *err = "invalid value";
        return NGX_ERROR;
    }

    if (*expires == NGX_HTTP_EXPIRES_DAILY
        && *expires_time > 24 * 60 * 60)
    {
        *err = "daily time value must be less than 24 hours";
        return NGX_ERROR;
    }

    if (minus) {
        *expires_time = - *expires_time;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_add_header(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    ngx_table_elt_t  *h;

    if (value->len) {
        h = ngx_list_push(&r->headers_out.headers);
        if (h == NULL) {
            return NGX_ERROR;
        }

        h->hash = 1;
        h->key = hv->key;
        h->value = *value;
    } else {
        for (ngx_list_part_t *part = &r->headers_out.headers.part; part; part = part->next) {
            ngx_table_elt_t *elts = part->elts;

            for (ngx_uint_t i = 0; i < part->nelts; i++) {
                if (elts[i].value.len && (hv->key.len == elts[i].key.len || hv->key.data[hv->key.len - 1] == '*') && !ngx_strncasecmp(hv->key.data, elts[i].key.data, hv->key.data[hv->key.len - 1] == '*' ? hv->key.len - 1 : hv->key.len)) {
                    elts[i].hash = 0;
                    elts[i].value = *value;
                }
            }
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_add_input_header(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    ngx_table_elt_t  *h;
    ngx_http_core_loc_conf_t *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

    if (value->len) {
        h = ngx_list_push(&r->headers_in.headers);
        if (h == NULL) {
            return NGX_ERROR;
        }

        h->hash = 1;
        h->key = hv->key;
        h->value = *value;
    } else if (!clcf->internal && !clcf->named) {
        for (ngx_list_part_t *part = &r->headers_in.headers.part; part; part = part->next) {
            ngx_table_elt_t *elts = part->elts;

            for (ngx_uint_t i = 0; i < part->nelts; i++) {
                if (elts[i].value.len && (hv->key.len == elts[i].key.len || hv->key.data[hv->key.len - 1] == '*') && !ngx_strncasecmp(hv->key.data, elts[i].key.data, hv->key.data[hv->key.len - 1] == '*' ? hv->key.len - 1 : hv->key.len)) {
                    elts[i].hash = 0;
                    elts[i].value = *value;
                }
            }
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_add_multi_header_lines(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    ngx_array_t      *pa;
    ngx_table_elt_t  *h, **ph;

    if (value->len == 0) {
        return NGX_OK;
    }

    pa = (ngx_array_t *) ((char *) &r->headers_out + hv->offset);

    if (pa->elts == NULL) {
        if (ngx_array_init(pa, r->pool, 1, sizeof(ngx_table_elt_t *)) != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    h = ngx_list_push(&r->headers_out.headers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    h->hash = 1;
    h->key = hv->key;
    h->value = *value;

    ph = ngx_array_push(pa);
    if (ph == NULL) {
        return NGX_ERROR;
    }

    *ph = h;

    return NGX_OK;
}


static ngx_int_t
ngx_http_add_multi_input_header_lines(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    ngx_array_t      *pa;
    ngx_table_elt_t  *h, **ph;

    if (value->len == 0) {
        return NGX_OK;
    }

    pa = (ngx_array_t *) ((char *) &r->headers_in + hv->offset);

    if (pa->elts == NULL) {
        if (ngx_array_init(pa, r->pool, 1, sizeof(ngx_table_elt_t *)) != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    h = ngx_list_push(&r->headers_in.headers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    h->hash = 1;
    h->key = hv->key;
    h->value = *value;

    ph = ngx_array_push(pa);
    if (ph == NULL) {
        return NGX_ERROR;
    }

    *ph = h;

    return NGX_OK;
}


static ngx_int_t
ngx_http_set_last_modified(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    if (ngx_http_set_response_header(r, hv, value) != NGX_OK) {
        return NGX_ERROR;
    }

    r->headers_out.last_modified_time =
        (value->len) ? ngx_parse_http_time(value->data, value->len) : -1;

    return NGX_OK;
}


static ngx_int_t
ngx_http_set_accept_ranges(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    if (value->len == 0) {
        r->allow_ranges = 0;
    }

    return ngx_http_set_response_header(r, hv, value);
}


static ngx_int_t
ngx_http_set_content_length(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    off_t           len;

    if (value->len == 0) {
        r->headers_out.content_length_n = -1;
        return ngx_http_set_response_header(r, hv, value);
    }

    len = ngx_atosz(value->data, value->len);
    if (len == NGX_ERROR) {
        return NGX_ERROR;
    }

    r->headers_out.content_length_n = len;

    return ngx_http_set_response_header(r, hv, value);
}


static ngx_int_t
ngx_http_set_content_type_header(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    u_char          *p, *last, *end;

    r->headers_out.content_type_len = value->len;
    r->headers_out.content_type = *value;
    r->headers_out.content_type_hash = 1;
    r->headers_out.content_type_lowcase = NULL;

    p = value->data;
    end = p + value->len;

    for (; p != end; p++) {

        if (*p != ';') {
            continue;
        }

        last = p;

        while (*++p == ' ') { /* void */ }

        if (p == end) {
            break;
        }

        if (ngx_strncasecmp(p, (u_char *) "charset=", 8) != 0) {
            continue;
        }

        p += 8;

        r->headers_out.content_type_len = last - value->data;

        if (*p == '"') {
            p++;
        }

        last = end;

        if (*(last - 1) == '"') {
            last--;
        }

        r->headers_out.charset.len = last - p;
        r->headers_out.charset.data = p;

        break;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_set_response_header(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    ngx_table_elt_t  *h, **old;

    old = (ngx_table_elt_t **) ((char *) &r->headers_out + hv->offset);

    if (value->len == 0) {
        if (*old) {
            (*old)->hash = 0;
            (*old)->value = *value;
            *old = NULL;
        } else {
            h = ngx_list_push(&r->headers_out.headers);
            if (h == NULL) {
                return NGX_ERROR;
            }

            *old = h;
            h->hash = 0;
            h->value = *value;
        }

        return NGX_OK;
    }

    if (*old) {
        h = *old;

    } else {
        h = ngx_list_push(&r->headers_out.headers);
        if (h == NULL) {
            return NGX_ERROR;
        }

        *old = h;
    }

    h->hash = 1;
    h->key = hv->key;
    h->value = *value;

    return NGX_OK;
}


static ngx_int_t ngx_http_set_host_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    ngx_str_t host;

    if (value->len) {
        host= *value;

        if (ngx_http_headers_validate_host(&host, r->pool, 0) != NGX_OK) {
            return NGX_ERROR;
        }

        r->headers_in.server = host;

    } else {
        r->headers_in.server = *value;
    }

    return ngx_http_set_request_header(r, hv, value);
}


static ngx_int_t ngx_http_set_connection_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    r->headers_in.connection_type = 0;

    if (value->len == 0) {
        return ngx_http_set_request_header(r, hv, value);
    }

    if (ngx_strcasestrn(value->data, "close", 5 - 1)) {
        r->headers_in.connection_type = NGX_HTTP_CONNECTION_CLOSE;
        r->headers_in.keep_alive_n = -1;

    } else if (ngx_strcasestrn(value->data, "keep-alive", 10 - 1)) {
        r->headers_in.connection_type = NGX_HTTP_CONNECTION_KEEP_ALIVE;
    }

    return ngx_http_set_request_header(r, hv, value);
}


static ngx_int_t ngx_http_set_user_agent_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    u_char  *user_agent, *msie;

    /* clear existing settings */

    r->headers_in.msie = 0;
    r->headers_in.msie6 = 0;
    r->headers_in.opera = 0;
    r->headers_in.gecko = 0;
    r->headers_in.chrome = 0;
    r->headers_in.safari = 0;
    r->headers_in.konqueror = 0;

    if (value->len == 0) {
        return ngx_http_set_request_header(r, hv, value);
    }

    /* check some widespread browsers */

    user_agent = value->data;

    msie = ngx_strstrn(user_agent, "MSIE ", 5 - 1);

    if (msie && msie + 7 < user_agent + value->len) {

        r->headers_in.msie = 1;

        if (msie[6] == '.') {

            switch (msie[5]) {
            case '4':
            case '5':
                r->headers_in.msie6 = 1;
                break;
            case '6':
                if (ngx_strstrn(msie + 8, "SV1", 3 - 1) == NULL) {
                    r->headers_in.msie6 = 1;
                }
                break;
            }
        }
    }

    if (ngx_strstrn(user_agent, "Opera", 5 - 1)) {
        r->headers_in.opera = 1;
        r->headers_in.msie = 0;
        r->headers_in.msie6 = 0;
    }

    if (!r->headers_in.msie && !r->headers_in.opera) {

        if (ngx_strstrn(user_agent, "Gecko/", 6 - 1)) {
            r->headers_in.gecko = 1;

        } else if (ngx_strstrn(user_agent, "Chrome/", 7 - 1)) {
            r->headers_in.chrome = 1;

        } else if (ngx_strstrn(user_agent, "Safari/", 7 - 1)
                   && ngx_strstrn(user_agent, "Mac OS X", 8 - 1))
        {
            r->headers_in.safari = 1;

        } else if (ngx_strstrn(user_agent, "Konqueror", 9 - 1)) {
            r->headers_in.konqueror = 1;
        }
    }

    return ngx_http_set_request_header(r, hv, value);
}


static ngx_int_t ngx_http_set_content_length_header(ngx_http_request_t *r,
    ngx_http_header_val_t *hv, ngx_str_t *value)
{
    off_t           len;

    if (value->len == 0) {
        return ngx_http_set_request_header(r, hv, value);
    }

    len = ngx_atosz(value->data, value->len);
    if (len == NGX_ERROR) {
        return NGX_ERROR;
    }

    r->headers_in.content_length_n = len;

    return ngx_http_set_request_header(r, hv, value);
}


static ngx_int_t
ngx_http_headers_validate_host(ngx_str_t *host, ngx_pool_t *pool,
    ngx_uint_t alloc)
{
    u_char  *h, ch;
    size_t   i, dot_pos, host_len;

    enum {
        sw_usual = 0,
        sw_literal,
        sw_rest
    } state;

    dot_pos = host->len;
    host_len = host->len;

    h = host->data;

    state = sw_usual;

    for (i = 0; i < host->len; i++) {
        ch = h[i];

        switch (ch) {

        case '.':
            if (dot_pos == i - 1) {
                return NGX_DECLINED;
            }
            dot_pos = i;
            break;

        case ':':
            if (state == sw_usual) {
                host_len = i;
                state = sw_rest;
            }
            break;

        case '[':
            if (i == 0) {
                state = sw_literal;
            }
            break;

        case ']':
            if (state == sw_literal) {
                host_len = i + 1;
                state = sw_rest;
            }
            break;

        case '\0':
            return NGX_DECLINED;

        default:

            if (ngx_path_separator(ch)) {
                return NGX_DECLINED;
            }

            if (ch >= 'A' && ch <= 'Z') {
                alloc = 1;
            }

            break;
        }
    }

    if (dot_pos == host_len - 1) {
        host_len--;
    }

    if (host_len == 0) {
        return NGX_DECLINED;
    }

    if (alloc) {
        host->data = ngx_pnalloc(pool, host_len);
        if (host->data == NULL) {
            return NGX_ERROR;
        }

        ngx_strlow(host->data, h, host_len);
    }

    host->len = host_len;

    return NGX_OK;
}


static ngx_int_t
ngx_http_set_request_header(ngx_http_request_t *r, ngx_http_header_val_t *hv,
    ngx_str_t *value)
{
    ngx_table_elt_t  *h, **old;

    old = (ngx_table_elt_t **) ((char *) &r->headers_in + hv->offset);

    if (value->len == 0) {
        if (*old) {
            (*old)->hash = 0;
            (*old)->value = *value;
            *old = NULL;
        }

        return NGX_OK;
    }

    if (*old) {
        h = *old;

    } else {
        h = ngx_list_push(&r->headers_in.headers);
        if (h == NULL) {
            return NGX_ERROR;
        }

        *old = h;
    }

    h->hash = 1;
    h->key = hv->key;
    h->value = *value;

    return NGX_OK;
}


static ngx_int_t ngx_http_headers_handler(ngx_http_request_t *r) {
    ngx_http_headers_conf_t *hcf = ngx_http_get_module_loc_conf(r, ngx_http_headers_filter_module);

    if (hcf->input_headers != NGX_CONF_UNSET_PTR) {
        ngx_http_header_val_t *h = hcf->input_headers->elts;
        for (ngx_uint_t i = 0; i < hcf->input_headers->nelts; i++) {
            ngx_str_t value;

            if (ngx_http_complex_value(r, &h[i].value, &value) != NGX_OK) {
                return NGX_ERROR;
            }

            if (h[i].handler(r, &h[i], &value) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    return NGX_DECLINED;
}


static void *
ngx_http_headers_create_conf(ngx_conf_t *cf)
{
    ngx_http_headers_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_headers_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     conf->expires_time = 0;
     *     conf->expires_value = NULL;
     */

    conf->expires = NGX_HTTP_EXPIRES_UNSET;
    conf->input_headers = NGX_CONF_UNSET_PTR;
    conf->headers = NGX_CONF_UNSET_PTR;
    conf->trailers = NGX_CONF_UNSET_PTR;
    conf->subrequest = NGX_CONF_UNSET;

    return conf;
}


/*static void ngx_conf_merge_header_value(ngx_array_t **conf, ngx_array_t **prev, void *cmp) {
    if (*conf == cmp || (*conf)->nelts == 0) {
        *conf = *prev;
    } else if (*prev != cmp && (*prev)->nelts) {
        ngx_uint_t orig_len = (*conf)->nelts;
        (void) ngx_array_push_n(*conf, (*prev)->nelts);
        ngx_http_header_val_t *elts = (*conf)->elts;
        for (ngx_uint_t i = 0; i < orig_len; i++) {
            elts[(*conf)->nelts - 1 - i] = elts[orig_len - 1 - i];
        }
        ngx_http_header_val_t *prev_elts = (*prev)->elts;
        for (ngx_uint_t i = 0; i < (*prev)->nelts; i++) {
            elts[i] = prev_elts[i];
        }
    }
}*/


static char *
ngx_http_headers_merge_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_headers_conf_t *prev = parent;
    ngx_http_headers_conf_t *conf = child;

    if (conf->expires == NGX_HTTP_EXPIRES_UNSET) {
        conf->expires = prev->expires;
        conf->expires_time = prev->expires_time;
        conf->expires_value = prev->expires_value;

        if (conf->expires == NGX_HTTP_EXPIRES_UNSET) {
            conf->expires = NGX_HTTP_EXPIRES_OFF;
        }
    }

    ngx_conf_merge_array_value(&conf->input_headers, &prev->input_headers, NGX_CONF_UNSET_PTR);
    ngx_conf_merge_array_value(&conf->headers, &prev->headers, NGX_CONF_UNSET_PTR);
    ngx_conf_merge_array_value(&conf->trailers, &prev->trailers, NGX_CONF_UNSET_PTR);
    ngx_conf_merge_value(conf->subrequest, prev->subrequest, 0);

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_headers_filter_init(ngx_conf_t *cf)
{
    ngx_http_next_header_filter = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_headers_filter;

    ngx_http_next_body_filter = ngx_http_top_body_filter;
    ngx_http_top_body_filter = ngx_http_trailers_filter;

    ngx_http_headers_main_conf_t *hmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_headers_filter_module);
    if (!hmcf->requires_handler) {
        return NGX_OK;
    }

    ngx_http_core_main_conf_t *cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    ngx_http_handler_pt *h = ngx_array_push(&cmcf->phases[NGX_HTTP_REWRITE_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_headers_handler;

    return NGX_OK;
}


static void *
ngx_http_headers_filter_main(ngx_conf_t *cf)
{
    ngx_http_headers_main_conf_t *hmcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_headers_main_conf_t));

    if (hmcf == NULL) {
        return NULL;
    }

    return hmcf;
}


static char *
ngx_http_headers_expires(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_headers_conf_t *hcf = conf;

    char                              *err;
    ngx_str_t                         *value;
    ngx_int_t                          rc;
    ngx_uint_t                         n;
    ngx_http_complex_value_t           cv;
    ngx_http_compile_complex_value_t   ccv;

    if (hcf->expires != NGX_HTTP_EXPIRES_UNSET) {
        return "is duplicate";
    }

    value = cf->args->elts;

    if (cf->args->nelts == 2) {

        hcf->expires = NGX_HTTP_EXPIRES_ACCESS;

        n = 1;

    } else { /* cf->args->nelts == 3 */

        if (ngx_strcmp(value[1].data, "modified") != 0) {
            return "invalid value";
        }

        hcf->expires = NGX_HTTP_EXPIRES_MODIFIED;

        n = 2;
    }

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[n];
    ccv.complex_value = &cv;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if (cv.lengths != NULL) {

        hcf->expires_value = ngx_palloc(cf->pool,
                                        sizeof(ngx_http_complex_value_t));
        if (hcf->expires_value == NULL) {
            return NGX_CONF_ERROR;
        }

        *hcf->expires_value = cv;

        return NGX_CONF_OK;
    }

    rc = ngx_http_parse_expires(&value[n], &hcf->expires, &hcf->expires_time,
                                &err);
    if (rc != NGX_OK) {
        return err;
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_headers_add(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_headers_conf_t *hcf = conf;

    ngx_str_t                          *elts;
    ngx_uint_t                          i;
    ngx_array_t                       **headers;
    ngx_http_header_val_t              *hv = NULL;
    ngx_http_set_header_t              *set;
    ngx_http_compile_complex_value_t    ccv;

    elts = cf->args->elts;

    headers = (ngx_array_t **) ((char *) hcf + cmd->offset);

    if (*headers == NGX_CONF_UNSET_PTR) {
        *headers = ngx_array_create(cf->pool, 1,
                                    sizeof(ngx_http_header_val_t));
        if (*headers == NULL) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_create"); return NGX_CONF_ERROR; }
    }

    if (headers == &hcf->headers && elts[2].len != 0 && elts[1].data[elts[1].len - 1] == '*') { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "wildchar not alowed with non-empty value \"%V\"", &elts[1]); return NGX_CONF_ERROR; }

    if (headers == &hcf->headers) {
        set = ngx_http_set_headers;
        for (i = 0; set[i].name.len; i++) {
            if ((elts[1].len == set[i].name.len || elts[1].data[elts[1].len - 1] == '*') && !ngx_strncasecmp(elts[1].data, set[i].name.data, elts[1].data[elts[1].len - 1] == '*' ? elts[1].len - 1 : elts[1].len)) {
                if (!(hv = ngx_array_push(*headers))) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_push"); return NGX_CONF_ERROR; }

                hv->key = elts[1];
                hv->offset = set[i].offset;
                hv->always = 0;
                hv->handler = set[i].handler;

                if (elts[2].len == 0) {
                    ngx_memzero(&hv->value, sizeof(ngx_http_complex_value_t));

                } else {
                    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

                    ccv.cf = cf;
                    ccv.value = &elts[2];
                    ccv.complex_value = &hv->value;

                    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_http_compile_complex_value"); return NGX_CONF_ERROR; }
                }

                if (cf->args->nelts == 4) {
                    if (ngx_strncasecmp(elts[3].data, (u_char *)"always", sizeof("always") - 1) != 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &elts[3]); return NGX_CONF_ERROR; }
                    hv->always = 1;
                }

                if (elts[1].data[elts[1].len - 1] != '*') break;
            }
        }
    }

    if (!hv) {
        if (!(hv = ngx_array_push(*headers))) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_push"); return NGX_CONF_ERROR; }

        hv->key = elts[1];
        hv->offset = 0;
        hv->always = 0;
        hv->handler = headers == &hcf->headers ? ngx_http_add_header : NULL;

        if (elts[2].len == 0) {
            ngx_memzero(&hv->value, sizeof(ngx_http_complex_value_t));

        } else {
            ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

            ccv.cf = cf;
            ccv.value = &elts[2];
            ccv.complex_value = &hv->value;

            if (ngx_http_compile_complex_value(&ccv) != NGX_OK) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_http_compile_complex_value"); return NGX_CONF_ERROR; }
        }

        if (cf->args->nelts == 4) {
            if (ngx_strncasecmp(elts[3].data, (u_char *)"always", sizeof("always") - 1) != 0) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &elts[3]); return NGX_CONF_ERROR; }
            hv->always = 1;
        }
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_headers_add_input(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_headers_conf_t *hcf = conf;

    ngx_str_t                          *elts;
    ngx_uint_t                          i;
    ngx_array_t                       **headers;
    ngx_http_header_val_t              *hv = NULL;
    ngx_http_set_header_t              *set;
    ngx_http_compile_complex_value_t    ccv;

    elts = cf->args->elts;

    headers = (ngx_array_t **) ((char *) hcf + cmd->offset);

    if (*headers == NGX_CONF_UNSET_PTR) {
        *headers = ngx_array_create(cf->pool, 1,
                                    sizeof(ngx_http_header_val_t));
        if (*headers == NULL) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_create"); return NGX_CONF_ERROR; }
    }

    if (elts[2].len != 0 && elts[1].data[elts[1].len - 1] == '*') { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "wildchar not alowed with non-empty value \"%V\"", &elts[1]); return NGX_CONF_ERROR; }

    set = ngx_http_set_input_headers;
    for (i = 0; set[i].name.len; i++) {
        if ((elts[1].len == set[i].name.len || elts[1].data[elts[1].len - 1] == '*') && !ngx_strncasecmp(elts[1].data, set[i].name.data, elts[1].data[elts[1].len - 1] == '*' ? elts[1].len - 1 : elts[1].len)) {
            if (!(hv = ngx_array_push(*headers))) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_push"); return NGX_CONF_ERROR; }

            hv->key = elts[1];
            hv->offset = set[i].offset;
            hv->handler = set[i].handler;

            if (elts[2].len == 0) {
                ngx_memzero(&hv->value, sizeof(ngx_http_complex_value_t));

            } else {
                ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

                ccv.cf = cf;
                ccv.value = &elts[2];
                ccv.complex_value = &hv->value;

                if (ngx_http_compile_complex_value(&ccv) != NGX_OK) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_http_compile_complex_value"); return NGX_CONF_ERROR; }
            }

            if (elts[1].data[elts[1].len - 1] != '*') break;
        }
    }

    if (!hv) {
        if (!(hv = ngx_array_push(*headers))) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_array_push"); return NGX_CONF_ERROR; }

        hv->key = elts[1];
        hv->offset = 0;
        hv->handler = ngx_http_add_input_header;

        if (elts[2].len == 0) {
            ngx_memzero(&hv->value, sizeof(ngx_http_complex_value_t));

        } else {
            ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

            ccv.cf = cf;
            ccv.value = &elts[2];
            ccv.complex_value = &hv->value;

            if (ngx_http_compile_complex_value(&ccv) != NGX_OK) { ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "!ngx_http_compile_complex_value"); return NGX_CONF_ERROR; }
        }
    }

    ngx_http_headers_main_conf_t *hmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_headers_filter_module);
    hmcf->requires_handler = 1;

    return NGX_CONF_OK;
}
