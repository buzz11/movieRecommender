$(document).ready(function() {
		$.ajax({
				url: '{{ url_for("main.autocomplete") }}'
				}).done(function (data){
						$('#movie_autocomplete').autocomplete({
								source: data,
								minLength: 2
						});
				});
		});
