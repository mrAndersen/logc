FROM php:7.2.10-cli-stretch

RUN apt-get update && apt-get install -y git

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer
ENV COMPOSER_ALLOW_SUPERUSER=1


COPY ./ /usr/local/logc/
RUN composer install -d /usr/local/logc --optimize-autoloader
RUN docker-php-ext-install sockets

CMD php /usr/local/logc/logc --configuration=/usr/local/logc/logc.ini
#CMD tail -f /dev/null