FROM php:7.2.10-cli-stretch

RUN apt-get update && apt-get install -y git zlib1g-dev
RUN docker-php-ext-install sockets zip

ARG ENV='dev'

# xdebug
RUN if [ ${ENV} = 'dev' ]; then \
    pecl install xdebug-2.6.0 \
    && docker-php-ext-enable xdebug \
    && echo "xdebug.remote_enable=on" >> /usr/local/etc/php/conf.d/xdebug.ini \
    && echo "xdebug.force_display_errors=1" >> /usr/local/etc/php/conf.d/xdebug.ini \
    && echo "xdebug.extended_info=1" >> /usr/local/etc/php/conf.d/xdebug.ini \
    && echo "xdebug.remote_autostart=off" >> /usr/local/etc/php/conf.d/xdebug.ini \
;fi

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer
ENV COMPOSER_ALLOW_SUPERUSER=1

COPY ./ /usr/local/logc/
WORKDIR /usr/local/logc

RUN composer install --optimize-autoloader

EXPOSE 914

#CMD php /usr/local/logc/logc --configuration=/usr/local/logc/logc.yml
CMD tail -f /dev/null