$(document).ready(function() {
		$.ajax({
				url: '_autocomplete'
				}).done(function (data){
						$('#movie_autocomplete').autocomplete({
								source: data,
								minLength: 2
						});
				});
		});
